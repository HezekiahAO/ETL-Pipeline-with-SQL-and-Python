"""
Pipeline orchestration + CLI.

    python -m etl.pipeline init [--reset]
    python -m etl.pipeline run data/raw_orders_full.xml --mode full --batch-size 100
    python -m etl.pipeline run data/raw_orders_incremental.xml --mode incremental
    python -m etl.pipeline validate --expected data/expected_full.json
    python -m etl.pipeline abandon data/raw_orders_truncated.xml     # retire a checkpoint that will never be resumed

Flow per run
------------
  RAW        file archived in raw.source_files (once per distinct sha256)
  loop       parse XML (streaming) -> group into batches of N records
  per batch  ONE transaction:  staging insert -> etl.process_batch() [clean, dedupe, transform, load]
                               -> checkpoint update.  Commit.  (any error => whole batch rolled back)
  end        advance watermark, mark checkpoint completed, close run row.
"""
from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from psycopg2.extras import execute_values

from . import __version__
from .config import Settings
from .db import connect, init_schema
from .extract import RawOrder, file_sha256, iter_orders

log = logging.getLogger("etl.pipeline")


class PipelineError(RuntimeError):
    pass


class InjectedFailure(RuntimeError):
    """Raised on purpose by --fail-at-batch to demonstrate rollback + checkpoint recovery."""


@dataclass
class RunContext:
    run_id: int
    file_id: int
    sha256: str
    resume_after_position: int = 0
    resume_after_batch: int = 0
    resumed_from_run: int | None = None
    watermark: datetime | None = None


# ----------------------------------------------------------------------------- run start
def _start_run(conn, st: Settings, source: Path, sha: str, mode: str, batch_size: int) -> RunContext:
    """One short transaction: archive raw file, look for a checkpoint to resume, open the run row."""
    with conn, conn.cursor() as cur:
        cur.execute("SELECT file_id FROM raw.source_files WHERE sha256 = %s", (sha,))
        row = cur.fetchone()
        if row:
            file_id = row[0]
            log.info("raw file already archived (file_id=%s) - same sha256, not storing it again", file_id)
        else:
            cur.execute("INSERT INTO raw.source_files (file_name, sha256, size_bytes, raw_content) "
                        "VALUES (%s, %s, %s, %s) RETURNING file_id",
                        (source.name, sha, source.stat().st_size, source.read_text(encoding="utf-8-sig")))
            file_id = cur.fetchone()[0]
            log.info("raw file archived (file_id=%s)", file_id)

        cur.execute("SELECT last_batch_no, last_position, status, run_id FROM etl.checkpoint "
                    "WHERE pipeline_name = %s AND file_sha256 = %s", (st.pipeline_name, sha))
        cp = cur.fetchone()
        resume = bool(cp and cp[2] == "in_progress" and cp[1] > 0)
        after_batch, after_pos, prev_run = (cp[0], cp[1], cp[3]) if resume else (0, 0, None)

        cur.execute("SELECT etl.start_run(%s, %s, %s, %s, %s, %s, %s)",
                    (st.pipeline_name, mode, source.name, sha, batch_size, prev_run, after_pos if resume else None))
        run_id = cur.fetchone()[0]
        if not resume:
            cur.execute("INSERT INTO etl.checkpoint (pipeline_name, file_sha256, status, run_id) "
                        "VALUES (%s, %s, 'in_progress', %s) "
                        "ON CONFLICT (pipeline_name, file_sha256) DO UPDATE "
                        "SET last_batch_no = 0, last_position = 0, status = 'in_progress', "
                        "    run_id = EXCLUDED.run_id, updated_at = now()", (st.pipeline_name, sha, run_id))
        cur.execute("SELECT high_watermark FROM etl.watermark WHERE pipeline_name = %s", (st.pipeline_name,))
        wm = cur.fetchone()

    ctx = RunContext(run_id, file_id, sha, after_pos, after_batch, prev_run, wm[0] if wm else None)
    if resume:
        log.warning("RESUMING run %s: checkpoint of failed run %s says batch %s / position %s are committed",
                    run_id, prev_run, after_batch, after_pos)
    return ctx


# --------------------------------------------------------------------------- one batch
def _process_batch(conn, st: Settings, ctx: RunContext, batch_no: int, records: list[RawOrder],
                   fail_at_batch: int | None) -> dict:
    first, last = records[0].position, records[-1].position
    try:
        with conn, conn.cursor() as cur:                      # <- the batch transaction
            cur.execute("INSERT INTO etl.batch_log (run_id, batch_no, first_position, last_position, rows_in) "
                        "VALUES (%s, %s, %s, %s, %s) RETURNING batch_id",
                        (ctx.run_id, batch_no, first, last, len(records)))
            batch_id = cur.fetchone()[0]

            rows = [(ctx.run_id, batch_id, ctx.file_id, r.position, r.order_id, r.modified, r.customer_name,
                     r.customer_email, r.customer_country, r.order_date, r.status, r.channel) for r in records]
            stg = execute_values(
                cur, "INSERT INTO staging.orders_stg (run_id, batch_id, file_id, position, order_id_raw, modified_raw, "
                     "customer_name_raw, customer_email_raw, customer_country_raw, order_date_raw, status_raw, channel_raw) "
                     "VALUES %s RETURNING position, stg_id", rows, page_size=500, fetch=True)
            stg_by_pos = dict(stg)
            item_rows = [(stg_by_pos[r.position], i.line_no, i.sku, i.name, i.qty, i.price)
                         for r in records for i in r.items]
            if item_rows:
                execute_values(cur, "INSERT INTO staging.order_items_stg (stg_id, line_no, sku_raw, name_raw, qty_raw, price_raw) "
                                    "VALUES %s", item_rows, page_size=1000)

            cur.execute("SELECT etl.process_batch(%s)", (batch_id,))      # clean -> dedupe -> transform -> load
            stats = cur.fetchone()[0]
            cur.execute("SELECT etl.save_checkpoint(%s, %s, %s, %s, %s)",  # same tx => checkpoint never ahead of data
                        (st.pipeline_name, ctx.sha256, batch_no, last, ctx.run_id))

            if fail_at_batch == batch_no:                                  # fault injection: after all work, before COMMIT
                raise InjectedFailure(f"simulated crash in batch {batch_no} (positions {first}-{last}) before commit")
    except Exception as exc:
        # `with conn` already rolled the batch back. Record the failure in a NEW transaction.
        with conn, conn.cursor() as cur:
            cur.execute("SELECT etl.log_batch_failure(%s, %s, %s, %s, %s, %s)",
                        (ctx.run_id, batch_no, first, last, len(records), f"{type(exc).__name__}: {exc}"[:2000]))
        raise
    log.info("batch %-3s positions %5s-%-5s committed | in=%-4s inserted=%-4s updated=%-3s rejected=%-3s dup=%-3s stale=%-3s",
             batch_no, first, last, stats["rows_in"], stats["inserted"], stats["updated"],
             stats["rejected"], stats["duplicate"], stats["stale"])
    return stats


# ------------------------------------------------------------------------------ run
def run_pipeline(st: Settings, source: Path, mode: str = "incremental", batch_size: int | None = None,
                 fail_at_batch: int | None = None, purge_staging: bool = False) -> dict:
    batch_size = batch_size or st.batch_size
    if batch_size < 1:
        raise PipelineError("batch size must be >= 1")
    source = Path(source)
    if not source.is_file():
        raise PipelineError(f"source file not found: {source}")

    conn = connect(st)
    try:
        with conn, conn.cursor() as cur:                              # one run at a time per pipeline
            cur.execute("SELECT pg_try_advisory_lock(hashtext(%s))", (st.pipeline_name,))
            if not cur.fetchone()[0]:
                raise PipelineError("another run of this pipeline is active (advisory lock held)")

        sha = file_sha256(source)
        ctx = _start_run(conn, st, source, sha, mode, batch_size)
        log.info("run %s started | mode=%s file=%s batch_size=%s watermark=%s",
                 ctx.run_id, mode, source.name, batch_size, ctx.watermark)
        if mode == "incremental" and ctx.watermark is None:
            log.info("no watermark yet -> incremental behaves like a full load")

        extracted = skipped_wm = skipped_cp = 0
        max_modified: datetime | None = None
        batch_no = ctx.resume_after_batch
        buffer: list[RawOrder] = []
        in_batch = False                                                # True while a batch transaction is open
        horizon = datetime.now(timezone.utc) + timedelta(days=1)        # ignore absurd future timestamps
        try:
            for rec in iter_orders(source):
                extracted += 1
                ts = rec.modified_ts()
                if ts and ts <= horizon and (max_modified is None or ts > max_modified):
                    max_modified = ts
                if rec.position <= ctx.resume_after_position:             # checkpoint: already committed earlier
                    skipped_cp += 1
                    continue
                if mode == "incremental" and ctx.watermark and ts is not None and ts < ctx.watermark:
                    skipped_wm += 1                                       # watermark: nothing new here
                    continue
                buffer.append(rec)
                if len(buffer) >= batch_size:
                    batch_no += 1
                    in_batch = True
                    _process_batch(conn, st, ctx, batch_no, buffer, fail_at_batch)
                    in_batch, buffer = False, []
            if buffer:
                batch_no += 1
                in_batch = True
                _process_batch(conn, st, ctx, batch_no, buffer, fail_at_batch)
                in_batch = False
        except Exception as exc:
            conn.rollback()
            # a batch error belongs to batch_no; a parse error happens while assembling the NEXT batch
            failed_batch = batch_no if in_batch else batch_no + 1
            with conn, conn.cursor() as cur:
                cur.execute("SELECT etl.finish_run(%s, 'failed', %s, %s, %s, %s, %s)",
                            (ctx.run_id, extracted, skipped_wm, skipped_cp, f"{type(exc).__name__}: {exc}"[:2000], failed_batch))
            log.error("run %s FAILED in batch %s: %s: %s  (earlier batches stay committed; see etl.checkpoint)",
                      ctx.run_id, failed_batch, type(exc).__name__, exc)
            raise

        with conn, conn.cursor() as cur:                                 # success: close everything in ONE transaction
            cur.execute("UPDATE raw.source_files SET record_count = %s WHERE file_id = %s", (extracted, ctx.file_id))
            if max_modified:
                cur.execute("SELECT etl.advance_watermark(%s, %s, %s)", (st.pipeline_name, max_modified, ctx.run_id))
            cur.execute("UPDATE etl.checkpoint SET status = 'completed', updated_at = now(), run_id = %s "
                        "WHERE pipeline_name = %s AND file_sha256 = %s", (ctx.run_id, st.pipeline_name, sha))
            if purge_staging:
                cur.execute("DELETE FROM staging.orders_stg WHERE run_id = %s", (ctx.run_id,))
            cur.execute("SELECT etl.finish_run(%s, 'success', %s, %s, %s)", (ctx.run_id, extracted, skipped_wm, skipped_cp))
            cur.execute("SELECT * FROM etl.pipeline_runs WHERE run_id = %s", (ctx.run_id,))
            cols = [c[0] for c in cur.description]
            summary = dict(zip(cols, cur.fetchone()))
        log.info("run %s SUCCESS | extracted=%s skipped_watermark=%s skipped_checkpoint=%s staged=%s | "
                 "inserted=%s updated=%s rejected=%s duplicate=%s stale=%s | batches=%s",
                 ctx.run_id, extracted, skipped_wm, skipped_cp, summary["records_staged"], summary["records_inserted"],
                 summary["records_updated"], summary["records_rejected"], summary["records_duplicate"],
                 summary["records_stale"], summary["batches_completed"])
        return summary
    finally:
        conn.close()                                                      # also releases the advisory lock


def abandon_checkpoint(st: Settings, source: Path) -> int:
    """Retire the unfinished checkpoint of a file that will never be resumed (e.g. replaced by a corrected file)."""
    sha = file_sha256(Path(source))
    conn = connect(st)
    try:
        with conn, conn.cursor() as cur:
            cur.execute("SELECT etl.abandon_checkpoint(%s, %s)", (st.pipeline_name, sha))
            n = cur.fetchone()[0]
        log.info("%s checkpoint(s) abandoned for %s (sha256 %s...)", n, Path(source).name, sha[:12])
        return n
    finally:
        conn.close()


# ------------------------------------------------------------------------------ CLI
def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m etl.pipeline", description="XML -> PostgreSQL batch ETL")
    ap.add_argument("-v", "--verbose", action="store_true")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("init", help="create schemas/tables/functions and load reference data")
    p.add_argument("--reset", action="store_true", help="DROP everything first (demo only)")

    p = sub.add_parser("run", help="run the pipeline on one XML file")
    p.add_argument("source", type=Path)
    p.add_argument("--mode", choices=["full", "incremental"], default="incremental")
    p.add_argument("--batch-size", type=int)
    p.add_argument("--fail-at-batch", type=int, help="fault injection: crash inside this batch before commit")
    p.add_argument("--purge-staging", action="store_true", help="delete this run's staging rows after success")

    p = sub.add_parser("abandon", help="retire the unfinished checkpoint of a file that will not be resumed")
    p.add_argument("source", type=Path)

    p = sub.add_parser("validate", help="run data-quality / reconciliation checks")
    p.add_argument("--expected", type=Path, help="ground-truth JSON produced by scripts/generate_data.py")

    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S")
    st = Settings()
    log.info("etl %s | pipeline=%s", __version__, st.pipeline_name)
    try:
        if args.cmd == "init":
            conn = connect(st)
            try:
                init_schema(conn, st, reset=args.reset)
            finally:
                conn.close()
            return 0
        if args.cmd == "run":
            run_pipeline(st, args.source, args.mode, args.batch_size, args.fail_at_batch, args.purge_staging)
            return 0
        if args.cmd == "abandon":
            abandon_checkpoint(st, args.source)
            return 0
        if args.cmd == "validate":
            from .validate import run_validation
            return 0 if run_validation(st, args.expected) else 1
    except (PipelineError, InjectedFailure) as exc:
        log.error("%s", exc)
        return 1
    except Exception as exc:  # parse errors, DB errors, ...
        log.error("pipeline failed: %s: %s", type(exc).__name__, exc)
        return 1
    return 2


if __name__ == "__main__":
    sys.exit(main())
