"""Database helpers: connection, schema initialisation, reference data."""
from __future__ import annotations

import csv
import io
import logging
from pathlib import Path

import psycopg2

from .config import Settings

log = logging.getLogger("etl.db")

SCHEMA_FILES = ["01_schema.sql", "03_cleaning_functions.sql", "04_pipeline_functions.sql", "05_analysis_views.sql"]
REFERENCE_TABLES = {"country_map.csv": ("etl.country_map", ("alias", "iso2")),
                    "status_map.csv": ("etl.status_map", ("alias", "status")),
                    "channel_map.csv": ("etl.channel_map", ("alias", "channel"))}


def connect(settings: Settings):
    conn = psycopg2.connect(settings.dsn(), application_name="etl_orders_xml")
    conn.autocommit = False                 # we control every transaction explicitly
    return conn


def init_schema(conn, settings: Settings, reset: bool = False) -> None:
    """Create all layers + functions (idempotent) and (re)load reference CSVs."""
    with conn, conn.cursor() as cur:
        if reset:
            log.warning("RESET requested: dropping all pipeline schemas")
            cur.execute((settings.sql_dir / "00_reset.sql").read_text())
        for name in SCHEMA_FILES:
            log.info("applying %s", name)
            cur.execute((settings.sql_dir / name).read_text())
        for fname, (table, cols) in REFERENCE_TABLES.items():
            rows = list(csv.DictReader((settings.reference_dir / fname).open(newline="", encoding="utf-8")))
            buf = io.StringIO()
            w = csv.writer(buf)
            for r in rows:
                w.writerow([r[c] for c in cols])
            buf.seek(0)
            cur.execute(f"TRUNCATE {table}")                       # table name is a constant above, not user input
            cur.copy_expert(f"COPY {table} ({', '.join(cols)}) FROM STDIN WITH (FORMAT csv)", buf)
            log.info("reference data loaded: %s (%d rows)", table, len(rows))
