"""Validation: run sql/20_validation.sql and (optionally) compare with the generator's ground truth."""
from __future__ import annotations

import json
import logging
from decimal import Decimal
from pathlib import Path

from .config import Settings
from .db import connect

log = logging.getLogger("etl.validate")


def run_validation(st: Settings, expected_path: Path | None = None) -> bool:
    conn = connect(st)
    ok = True
    try:
        with conn, conn.cursor() as cur:
            cur.execute((st.sql_dir / "20_validation.sql").read_text())
            rows = cur.fetchall()
            print(f"\n{'#':>2}  {'RESULT':<6} CHECK / DETAIL")
            for ord_, name, passed, detail in rows:
                print(f"{ord_:>2}  {'PASS' if passed else 'FAIL':<6} {name}\n{'':12}{detail}")
                ok &= bool(passed)

            if expected_path:
                exp = json.loads(Path(expected_path).read_text())
                cur.execute("""SELECT (SELECT count(*) FROM dw.fact_orders),
                                      (SELECT coalesce(sum(order_total), 0) FROM dw.fact_orders),
                                      (SELECT count(DISTINCT customer_key) FROM dw.fact_orders),
                                      (SELECT count(DISTINCT product_key) FROM dw.fact_order_items)""")
                n_orders, revenue, n_cust, n_prod = cur.fetchone()
                cur.execute("SELECT status, count(*) FROM dw.fact_orders GROUP BY status")
                by_status = dict(cur.fetchall())
                print(f"\nGround-truth comparison ({Path(expected_path).name})")
                comparisons = [("valid unique orders", exp["valid_unique_orders"], n_orders),
                               ("total revenue", Decimal(exp["total_revenue"]), revenue),
                               ("distinct customers", exp["distinct_customers"], n_cust),
                               ("distinct products", exp["distinct_products"], n_prod),
                               ("orders by status", exp["orders_by_status"], by_status)]
                for label, want, got in comparisons:
                    good = want == got
                    ok &= good
                    print(f"    {'PASS' if good else 'FAIL'}  {label}: expected={want} actual={got}")
        print(f"\nOVERALL: {'ALL CHECKS PASSED' if ok else 'SOME CHECKS FAILED'}")
        return ok
    finally:
        conn.close()
