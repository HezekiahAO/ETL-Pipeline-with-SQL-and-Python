#!/usr/bin/env bash
# =====================================================================
# Reproduces every demonstration and writes the output to evidence/*.txt
#
#   export PGHOST=localhost PGUSER=etl_user PGPASSWORD=etl_pass PGDATABASE=etl_demo   (or DATABASE_URL)
#   ./scripts/run_demo.sh
#
# WARNING: drops and recreates the pipeline schemas in $PGDATABASE, and creates/drops
#          a second database (etl_sql_demo) for the pure-SQL demo.
# =====================================================================
set -uo pipefail
cd "$(dirname "$0")/.."
EV=evidence; mkdir -p "$EV"; rm -f "$EV"/*.txt
PY="python3 -m etl.pipeline"
P="psql -X -q"
fp()  { psql -X -At -F ' | ' -c "$(cat sql/21_fingerprint.sql)"; }
hdr() { printf '\n%s\n## %s\n%s\n' "==================================================================" "$1" "=================================================================="; }
FULL=data/raw_orders_full.xml; INC=data/raw_orders_incremental.xml
head -c 300000 "$FULL" > data/raw_orders_truncated.xml          # simulates a transfer cut off mid-file

# ---------------------------------------------------------------- 01
{
hdr "EVIDENCE 01 - INITIAL FULL LOAD  ($FULL, $(grep -c '<order[ >]' $FULL) <order> records, batch size 100)"
$PY init --reset
$PY run $FULL --mode full --batch-size 100
hdr "Pipeline metadata written by the run"; $P -f sql/22_metadata_queries.sql
hdr "Validation: counts, duplicates, missing values, loads + comparison with the generator's ground truth"
$PY validate --expected data/expected_full.json
hdr "Ground truth the generator designed into the file (independent of the pipeline)"; cat data/expected_full.json
} > $EV/01_full_load.txt 2>&1
fp > /tmp/fp_full.txt

# ---------------------------------------------------------------- 02
{
hdr "EVIDENCE 02 - IDEMPOTENCY: running the same file again must change nothing"
echo "warehouse fingerprint BEFORE:"; fp
hdr "re-run #1: same file, --mode full"; $PY run $FULL --mode full --batch-size 100
hdr "re-run #2: same file, --mode incremental"; $PY run $FULL --mode incremental --batch-size 100
hdr "re-run #3: same file, --mode full, different batch size (37)"; $PY run $FULL --mode full --batch-size 37
echo; echo "warehouse fingerprint AFTER 3 re-runs:"; fp
fp | diff - /tmp/fp_full.txt >/dev/null && echo ">>> IDENTICAL to the fingerprint after the first load: no duplicate rows, no changed rows"
hdr "What each run did (note inserted=0 updated=0 for runs 2-4)"
$P -c "SELECT run_id, run_mode AS mode, records_extracted AS extracted, records_skipped_watermark AS skip_wm, records_staged AS staged,
              records_inserted AS inserted, records_updated AS updated, records_rejected AS rejected, records_duplicate AS duplicate,
              records_stale AS stale, batches_completed AS batches FROM etl.pipeline_runs ORDER BY run_id"
$P -c "SELECT count(*) AS fact_orders, (SELECT count(*) FROM dw.fact_order_items) AS lines FROM dw.fact_orders"
} > $EV/02_idempotency.txt 2>&1

# ---------------------------------------------------------------- 03
N0=$(psql -X -At -c "SELECT count(*) FROM dw.fact_orders")
{
hdr "EVIDENCE 03 - INCREMENTAL LOAD  ($INC, $(grep -c '<order[ >]' $INC) records: new + changed + re-sent + junk)"
echo "fact_orders BEFORE: $N0"
psql -X -At -c "SELECT 'watermark before: ' || high_watermark FROM etl.watermark"
$PY run $INC --mode incremental --batch-size 50
RUN=$(psql -X -At -c "SELECT max(run_id) FROM etl.pipeline_runs")
echo; echo "fact_orders AFTER: $(psql -X -At -c 'SELECT count(*) FROM dw.fact_orders')"
hdr "Only new/changed rows were written: rows touched by run $RUN vs untouched"
$P -c "SELECT count(*) FILTER (WHERE first_loaded_run_id = $RUN) AS new_rows,
              count(*) FILTER (WHERE first_loaded_run_id < $RUN AND last_loaded_run_id = $RUN) AS changed_rows,
              count(*) FILTER (WHERE last_loaded_run_id < $RUN) AS untouched_rows FROM dw.fact_orders"
hdr "Sample of changed orders (version 2, status advanced)"
$P -c "SELECT f.order_id, f.status, r.version_no, f.first_loaded_run_id AS first_run, f.last_loaded_run_id AS last_run, f.order_total
       FROM dw.fact_orders f JOIN etl.source_registry r ON r.source_key = f.order_id
       WHERE f.first_loaded_run_id < $RUN AND f.last_loaded_run_id = $RUN ORDER BY f.order_id LIMIT 6"
hdr "Run metadata + batches"; $P -c "SELECT * FROM etl.pipeline_runs WHERE run_id = $RUN" -x
$P -c "SELECT batch_no, first_position AS first, last_position AS last, status, rows_in AS in, rows_inserted AS ins, rows_updated AS upd, rows_rejected AS rej, rows_duplicate AS dup FROM etl.batch_log WHERE run_id = $RUN ORDER BY batch_no"
hdr "Validation + ground truth for the second export"; $PY validate --expected data/expected_incremental.json
hdr "Ground truth designed into the second file"; cat data/expected_incremental.json
} > $EV/03_incremental_load.txt 2>&1
fp > /tmp/fp_full_inc.txt

# ---------------------------------------------------------------- 04
{
hdr "EVIDENCE 04 - DEDUPLICATION"
$P -f sql/23_dedup_evidence.sql
hdr "Dedup registry: one row per source key; version_no > 1 = the record changed and was re-loaded"
$P -c "SELECT version_no, count(*) AS keys FROM etl.source_registry GROUP BY 1 ORDER BY 1"
hdr "Same content never loaded twice (must be 0)"
$P -c "SELECT count(*) AS order_hash_loaded_more_than_once FROM (SELECT order_id, record_hash FROM cleaned.orders WHERE load_action IS NOT NULL GROUP BY 1,2 HAVING count(*) > 1) x"
} > $EV/04_deduplication.txt 2>&1

# ---------------------------------------------------------------- 07 (pure SQL) + cross-check
{
hdr "EVIDENCE 07 - THE SAME ETL IN PURE SQL (xpath + plpgsql, no Python) - own database etl_sql_demo"
bash scripts/run_sql_demo.sh
hdr "CROSS-CHECK: Python pipeline warehouse vs pure-SQL warehouse (full + incremental loads, different batch sizes)"
echo "Python pipeline (db: ${PGDATABASE:-default}):"; fp
echo "Pure SQL        (db: etl_sql_demo):"; PGDATABASE=etl_sql_demo fp
[ "$(fp)" = "$(PGDATABASE=etl_sql_demo fp)" ] && echo ">>> IDENTICAL: two independent implementations produce exactly the same warehouse"
} > $EV/07_pure_sql_etl.txt 2>&1

# ---------------------------------------------------------------- 08 / 09
{ hdr "EVIDENCE 08 - ANALYSIS LAYER (queries on the final PostgreSQL tables)"; $P -f sql/24_analysis_queries.sql; } > $EV/08_analysis.txt 2>&1
{ hdr "EVIDENCE 09 - FINAL VALIDATION (after full + incremental loads)"; $PY validate --expected data/expected_incremental.json; } > $EV/09_final_validation.txt 2>&1

# ---------------------------------------------------------------- 05
{
hdr "EVIDENCE 05 - ERROR HANDLING + CHECKPOINT RESTART"
echo "Scenario: a crash happens inside batch 4 of a full load (fault injected after the batch's work, before COMMIT)."
$PY init --reset >/dev/null 2>&1
hdr "step 1: run with --fail-at-batch 4"
$PY run $FULL --mode full --batch-size 100 --fail-at-batch 4; echo "process exit code: $?"
hdr "step 2: what is in the database after the crash?"
$P -c "SELECT count(*) AS orders_in_dw_after_crash FROM dw.fact_orders"
$P -c "SELECT count(*) AS staged_rows_of_failed_batch FROM staging.orders_stg s JOIN etl.batch_log b USING (batch_id) WHERE b.status = 'failed'"
$P -c "SELECT pipeline_name, last_batch_no, last_position, status AS checkpoint_status FROM etl.checkpoint"
$P -c "SELECT run_id, status, batches_completed, records_inserted, error_batch_no, error_message FROM etl.pipeline_runs" -x
$P -c "SELECT batch_no, first_position AS first, last_position AS last, status, rows_in AS in, error_message FROM etl.batch_log ORDER BY batch_no"
hdr "step 3: restart = run exactly the same command again (no flags, no manual cleanup)"
$PY run $FULL --mode full --batch-size 100; echo "process exit code: $?"
hdr "step 4: metadata after recovery"; $P -f sql/22_metadata_queries.sql
hdr "step 5: is the recovered warehouse identical to an uninterrupted load? (fingerprint vs evidence 01)"
fp
fp | diff - /tmp/fp_full.txt >/dev/null && echo ">>> IDENTICAL to the clean full load of evidence 01"
hdr "step 6: validation"; $PY validate --expected data/expected_full.json
} > $EV/05_failure_and_checkpoint_resume.txt 2>&1

# ---------------------------------------------------------------- 06
{
hdr "EVIDENCE 06 - REAL FAULT: XML FILE TRUNCATED IN TRANSFER, THEN RE-DELIVERED"
$PY init --reset >/dev/null 2>&1
echo "data/raw_orders_truncated.xml = first 300,000 bytes of the full file (cut mid-element)"
hdr "step 1: run on the truncated file"
$PY run data/raw_orders_truncated.xml --mode full --batch-size 100; echo "process exit code: $?"
hdr "step 2: state after the failure"
$P -c "SELECT run_id, status, records_extracted AS extracted, batches_completed, error_batch_no, left(error_message, 90) AS error FROM etl.pipeline_runs"
$P -c "SELECT count(*) AS orders_loaded_from_good_prefix FROM dw.fact_orders"
hdr "step 3: the complete file arrives (different sha256 => no checkpoint to reuse; the dedup registry is the safety net)"
$PY run $FULL --mode full --batch-size 100; echo "process exit code: $?"
$P -c "SELECT run_id, status, records_extracted AS extracted, records_staged AS staged, records_inserted AS inserted, records_updated AS updated, records_duplicate AS duplicate, records_stale AS stale, records_rejected AS rejected FROM etl.pipeline_runs ORDER BY run_id"
hdr "step 4: operator retires the checkpoint of the broken file (it will never be resumed)"
$PY abandon data/raw_orders_truncated.xml
$P -c "SELECT left(file_sha256,12) AS file_sha, last_batch_no, last_position, status FROM etl.checkpoint ORDER BY updated_at"
hdr "step 5: result identical to an uninterrupted load?"
fp
fp | diff - /tmp/fp_full.txt >/dev/null && echo ">>> IDENTICAL to the clean full load of evidence 01"
hdr "step 6: validation"; $PY validate --expected data/expected_full.json
} > $EV/06_truncated_xml_recovery.txt 2>&1

# ---------------------------------------------------------------- 11
{
hdr "EVIDENCE 11 - HARD KILL: the process gets SIGKILL mid-run (no exception handler, no cleanup code can run)"
$PY init --reset >/dev/null 2>&1
$PY run $FULL --mode full --batch-size 3 > /tmp/kill_run.log 2>&1 &
PID=$!
until [ "$(psql -X -At -c 'SELECT coalesce(max(last_batch_no),0) FROM etl.checkpoint' 2>/dev/null || echo 0)" -ge 40 ]; do sleep 0.05; done
kill -9 $PID 2>/dev/null && echo "sent SIGKILL to the pipeline process (pid $PID)"; wait $PID 2>/dev/null
echo "last line the process logged before dying:"; tail -1 /tmp/kill_run.log | cut -c1-150
hdr "state left behind"
$P -c "SELECT run_id, status, ended_at IS NULL AS no_end_time, batches_completed FROM etl.pipeline_runs"
$P -c "SELECT last_batch_no, last_position, status FROM etl.checkpoint"
$P -c "SELECT count(*) AS orders_in_dw, (SELECT count(*) FROM staging.orders_stg s JOIN etl.batch_log b USING (batch_id) WHERE b.status = 'running') AS orphan_staged_rows FROM dw.fact_orders"
hdr "restart: same command (and a different batch size - checkpoints are position based)"
$PY run $FULL --mode full --batch-size 100
$P -c "SELECT run_id, status, left(error_message, 62) AS error, resumed_from_run_id AS resumed_from, records_skipped_checkpoint AS skipped_by_checkpoint FROM etl.pipeline_runs"
hdr "identical to an uninterrupted load?"
fp
fp | diff - /tmp/fp_full.txt >/dev/null && echo ">>> IDENTICAL to the clean full load of evidence 01"
hdr "validation"; $PY validate --expected data/expected_full.json
} > $EV/11_hard_kill_sigkill.txt 2>&1

# ---------------------------------------------------------------- 10
{ hdr "EVIDENCE 10 - AUTOMATED TESTS (pytest)"; python3 --version; psql --version
  ETL_TEST_DATABASE_URL="${ETL_TEST_DATABASE_URL:-}" python3 -m pytest tests -v 2>&1; } > $EV/10_tests.txt 2>&1

echo "evidence written to $EV/:"; ls -1 $EV
