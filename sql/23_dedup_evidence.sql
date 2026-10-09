-- How every staged record was classified (all runs), and examples of each kind of duplicate.
\echo '--- cleaned.orders: outcome per record, per run ---'
SELECT run_id, dq_status, coalesce(dq_reason, '-') AS dq_reason, coalesce(load_action, '-') AS load_action, count(*) AS records
FROM cleaned.orders GROUP BY 1,2,3,4 ORDER BY 1,2,3,4;

\echo '--- example: one order sent several times in run 1 and what happened to each copy ---'
SELECT c.run_id, c.batch_id, c.position, c.order_id, c.modified_ts, c.status, c.dq_status, c.dq_reason, c.load_action,
       left(c.record_hash, 8) AS hash
FROM cleaned.orders c
WHERE c.order_id = (SELECT order_id FROM cleaned.orders WHERE run_id = (SELECT min(run_id) FROM etl.pipeline_runs)
                    GROUP BY order_id HAVING count(*) > 2 ORDER BY order_id LIMIT 1)
ORDER BY c.run_id, c.position;

\echo '--- rejected records by reason (first run) ---'
SELECT dq_reason, count(*) AS records, string_agg(coalesce(order_id,'(no id)'), ', ' ORDER BY position) FILTER (WHERE order_id IS NOT NULL) AS sample_ids
FROM cleaned.orders WHERE dq_status = 'rejected' AND run_id = (SELECT min(run_id) FROM etl.pipeline_runs) GROUP BY 1 ORDER BY 2 DESC;
