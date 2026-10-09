-- =====================================================================
-- 22_metadata_queries.sql - read the pipeline metadata
-- =====================================================================
\echo '--- etl.pipeline_runs: one row per run ---'
SELECT run_id, run_mode AS mode, source_file, status,
       to_char(started_at,'HH24:MI:SS') AS started, to_char(ended_at,'HH24:MI:SS') AS ended,
       records_extracted AS extracted, records_skipped_watermark AS skip_wm, records_skipped_checkpoint AS skip_cp,
       records_staged AS staged, records_inserted AS ins, records_updated AS upd, records_loaded AS loaded,
       records_rejected AS rej, records_duplicate AS dup, records_stale AS stale, batches_completed AS batches,
       resumed_from_run_id AS resumed_from
FROM etl.pipeline_runs ORDER BY run_id;

\echo '--- errors recorded ---'
SELECT run_id, error_batch_no AS batch, left(error_message, 110) AS error_message
FROM etl.pipeline_runs WHERE error_message IS NOT NULL ORDER BY run_id;

\echo '--- etl.batch_log (latest 14) ---'
SELECT run_id, batch_no, first_position AS first, last_position AS last, status, rows_in AS "in",
       rows_inserted AS ins, rows_updated AS upd, rows_rejected AS rej, rows_duplicate AS dup, rows_stale AS stale,
       left(coalesce(error_message,''), 60) AS error
FROM etl.batch_log ORDER BY batch_id DESC LIMIT 14;

\echo '--- etl.checkpoint ---'
SELECT pipeline_name, left(file_sha256, 12) AS file_sha, last_batch_no, last_position, status, run_id FROM etl.checkpoint;

\echo '--- etl.watermark ---'
SELECT pipeline_name, high_watermark, run_id FROM etl.watermark;

\echo '--- etl.source_registry (dedup registry) summary ---'
SELECT count(*) AS keys, count(*) FILTER (WHERE version_no > 1) AS keys_with_updates, max(version_no) AS max_version
FROM etl.source_registry;
