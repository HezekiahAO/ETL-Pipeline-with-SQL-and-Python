"""
End-to-end tests against a real PostgreSQL (disposable database, see conftest).
Each test proves one of the pipeline's guarantees.
"""
import json
from decimal import Decimal

import pytest

from etl.config import Settings
from etl.db import connect, init_schema
from etl.extract import file_sha256
from etl.pipeline import InjectedFailure, abandon_checkpoint, run_pipeline

from conftest import DATA, ROOT

FULL, INC = DATA / "raw_orders_full.xml", DATA / "raw_orders_incremental.xml"


@pytest.fixture()
def st(test_url):
    s = Settings(database_url=test_url)
    conn = connect(s)
    try:
        init_schema(conn, s, reset=True)
    finally:
        conn.close()
    return s


def q(st, sql, args=None):
    conn = connect(st)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql, args)
            return cur.fetchall()
    finally:
        conn.close()


def fingerprint(st):
    return q(st, (ROOT / "sql" / "21_fingerprint.sql").read_text())


def expected(name):
    return json.loads((DATA / name).read_text())


def test_full_load_matches_ground_truth(st):
    exp = expected("expected_full.json")
    run = run_pipeline(st, FULL, "full", batch_size=100)
    assert run["status"] == "success" and run["batches_completed"] == 11
    assert run["records_extracted"] == exp["records_in_file"]
    assert run["records_inserted"] == exp["valid_unique_orders"]          # every real order inserted exactly once
    assert run["records_rejected"] == exp["rejected_records"]
    (n, revenue), = q(st, "SELECT count(*), sum(order_total) FROM dw.fact_orders")
    assert n == exp["valid_unique_orders"] and revenue == Decimal(exp["total_revenue"])


def test_rerunning_same_file_is_idempotent(st):
    run_pipeline(st, FULL, "full", batch_size=100)
    before = fingerprint(st)
    again = run_pipeline(st, FULL, "full", batch_size=100)               # full again
    inc = run_pipeline(st, FULL, "incremental", batch_size=100)          # incremental on the same file
    assert (again["records_inserted"], again["records_updated"]) == (0, 0)
    assert (inc["records_inserted"], inc["records_updated"]) == (0, 0)
    assert fingerprint(st) == before


def test_incremental_loads_only_new_or_changed(st):
    run_pipeline(st, FULL, "full", batch_size=100)
    exp = expected("expected_incremental.json")
    run = run_pipeline(st, INC, "incremental", batch_size=50)
    assert run["records_skipped_watermark"] == exp["old_records_resent_skippable_by_watermark"]
    assert run["records_inserted"] == exp["new_orders"]
    assert run["records_updated"] == exp["changed_orders"]
    assert run["records_rejected"] == exp["rejected_records"]
    assert run["records_duplicate"] == exp["identical_resent_with_new_timestamp"] + exp["repeated_new_orders_in_file"]
    (n, revenue), = q(st, "SELECT count(*), sum(order_total) FROM dw.fact_orders")
    assert n == exp["valid_unique_orders"] and revenue == Decimal(exp["total_revenue"])


def test_crash_rolls_back_batch_then_resumes_from_checkpoint(st):
    run_pipeline(st, FULL, "full", batch_size=100)
    clean = fingerprint(st)
    init_schema(connect(st), st, reset=True)

    with pytest.raises(InjectedFailure):
        run_pipeline(st, FULL, "full", batch_size=100, fail_at_batch=4)
    # batches 1-3 committed, batch 4 left nothing behind
    assert q(st, "SELECT last_batch_no, last_position, status FROM etl.checkpoint") == [(3, 300, "in_progress")]
    assert q(st, "SELECT count(*) FROM staging.orders_stg s JOIN etl.batch_log b USING (batch_id) WHERE b.status = 'failed'") == [(0,)]
    assert q(st, "SELECT status, error_batch_no FROM etl.pipeline_runs") == [("failed", 4)]

    resumed = run_pipeline(st, FULL, "full", batch_size=100)
    assert resumed["records_skipped_checkpoint"] == 300 and resumed["resumed_from_run_id"] == 1
    assert fingerprint(st) == clean
    assert q(st, "SELECT status FROM etl.checkpoint") == [("completed",)]


def test_truncated_file_then_complete_file_recovers_via_dedup_registry(st, tmp_path):
    run_pipeline(st, FULL, "full", batch_size=100)
    clean = fingerprint(st)
    init_schema(connect(st), st, reset=True)

    cut = tmp_path / "cut.xml"
    cut.write_text(FULL.read_text()[:300_000])
    with pytest.raises(Exception):
        run_pipeline(st, cut, "full", batch_size=100)
    loaded = q(st, "SELECT count(*) FROM dw.fact_orders")[0][0]
    assert 0 < loaded < 900                                              # partial, but consistent, data

    run = run_pipeline(st, FULL, "full", batch_size=100)                  # different file hash => no checkpoint to reuse
    assert run["records_duplicate"] >= loaded                             # ... the registry recognises what is already loaded
    assert fingerprint(st) == clean
    assert q(st, "SELECT count(*) FROM etl.checkpoint WHERE status = 'in_progress'") == [(1,)]   # the broken file's leftover
    assert abandon_checkpoint(st, cut) == 1                                # operator retires it deliberately
    assert q(st, "SELECT count(*) FROM etl.checkpoint WHERE status = 'in_progress'") == [(0,)]


@pytest.mark.parametrize("size", [7, 333, 5000])
def test_result_does_not_depend_on_batch_size(st, size):
    run_pipeline(st, FULL, "full", batch_size=size)
    run_pipeline(st, INC, "incremental", batch_size=size)
    got = fingerprint(st)
    init_schema(connect(st), st, reset=True)
    run_pipeline(st, FULL, "full", batch_size=100)
    run_pipeline(st, INC, "incremental", batch_size=50)
    assert got == fingerprint(st)


def test_validation_suite_passes_after_loads(st):
    from etl.validate import run_validation
    run_pipeline(st, FULL, "full", batch_size=100)
    run_pipeline(st, INC, "incremental", batch_size=50)
    assert run_validation(st, DATA / "expected_incremental.json")
