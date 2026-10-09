-- =====================================================================
-- 20_validation.sql - ONE query returning (check_name, passed, detail).
-- Used by `python -m etl.pipeline validate` and by psql (evidence).
-- =====================================================================
WITH
recon1 AS (SELECT count(*) AS n FROM etl.pipeline_runs
           WHERE status = 'success'
             AND records_extracted <> records_skipped_watermark + records_skipped_checkpoint + records_staged),
recon2 AS (SELECT count(*) AS n FROM etl.pipeline_runs
           WHERE status = 'success'
             AND records_staged <> records_inserted + records_updated + records_rejected + records_duplicate + records_stale),
stg_unclean AS (SELECT count(*) AS n FROM staging.orders_stg s LEFT JOIN cleaned.orders c USING (stg_id) WHERE c.stg_id IS NULL),
pending AS (SELECT count(*) AS n FROM cleaned.orders WHERE dq_status = 'pending'),
reg AS (SELECT (SELECT count(*) FROM dw.fact_orders) AS facts, (SELECT count(*) FROM etl.source_registry) AS registry),
hash_mismatch AS (SELECT count(*) AS n FROM dw.fact_orders f JOIN etl.source_registry r ON r.source_key = f.order_id
                  WHERE f.record_hash <> r.record_hash),
dup_keys AS (SELECT (SELECT count(*) FROM (SELECT order_id FROM dw.fact_orders GROUP BY 1 HAVING count(*) > 1) a)
                  + (SELECT count(*) FROM (SELECT email FROM dw.dim_customer GROUP BY 1 HAVING count(*) > 1) b)
                  + (SELECT count(*) FROM (SELECT sku FROM dw.dim_product GROUP BY 1 HAVING count(*) > 1) c) AS n),
double_load AS (SELECT count(*) AS n FROM (SELECT order_id, record_hash FROM cleaned.orders
                WHERE load_action IS NOT NULL GROUP BY 1, 2 HAVING count(*) > 1) x),
nulls_req AS (SELECT count(*) AS n FROM dw.fact_orders
              WHERE order_id IS NULL OR customer_key IS NULL OR date_key IS NULL OR status IS NULL OR channel IS NULL
                 OR order_total IS NULL OR item_count IS NULL OR record_hash IS NULL),
nulls_dim AS (SELECT (SELECT count(*) FROM dw.dim_customer WHERE email IS NULL)
                   + (SELECT count(*) FROM dw.dim_product  WHERE sku IS NULL)
                   + (SELECT count(*) FROM dw.fact_order_items WHERE qty IS NULL OR unit_price IS NULL OR product_key IS NULL) AS n),
optional AS (SELECT (SELECT count(*) FROM dw.dim_customer WHERE name IS NULL)    AS cust_no_name,
                    (SELECT count(*) FROM dw.dim_customer WHERE country IS NULL) AS cust_no_country,
                    (SELECT count(*) FROM dw.dim_product  WHERE product_name IS NULL) AS prod_no_name,
                    (SELECT count(*) FROM dw.fact_orders  WHERE channel = 'unknown') AS orders_channel_unknown,
                    (SELECT count(*) FROM dw.fact_orders  WHERE status  = 'unknown') AS orders_status_unknown),
total_mismatch AS (SELECT count(*) AS n FROM dw.fact_orders o
                   WHERE o.order_total <> (SELECT coalesce(sum(line_total), -1) FROM dw.fact_order_items i WHERE i.order_id = o.order_id)
                      OR o.item_count  <> (SELECT count(*) FROM dw.fact_order_items i WHERE i.order_id = o.order_id)),
stuck AS (SELECT (SELECT count(*) FROM etl.batch_log WHERE status = 'running')
               + (SELECT count(*) FROM etl.pipeline_runs WHERE status = 'running') AS n),
open_cp AS (SELECT count(*) AS n FROM etl.checkpoint WHERE status = 'in_progress'),
failed_runs AS (SELECT count(*) AS n FROM etl.pipeline_runs WHERE status = 'failed'),
counts AS (SELECT (SELECT count(*) FROM dw.fact_orders) AS orders, (SELECT count(*) FROM dw.fact_order_items) AS lines,
                  (SELECT count(*) FROM dw.dim_customer) AS customers, (SELECT count(*) FROM dw.dim_product) AS products,
                  (SELECT count(*) FROM cleaned.orders WHERE dq_status = 'rejected') AS rejected_rows)
SELECT 1 AS ord, 'RECORD COUNTS: extracted = skipped + staged (every successful run)' AS check_name, n = 0 AS passed, 'violating runs: ' || n AS detail FROM recon1
UNION ALL SELECT 2, 'RECORD COUNTS: staged = inserted + updated + rejected + duplicate + stale', n = 0, 'violating runs: ' || n FROM recon2
UNION ALL SELECT 3, 'RECORD COUNTS: every staged row has a cleaned row', n = 0, 'staged rows never cleaned: ' || n FROM stg_unclean
UNION ALL SELECT 4, 'RECORD COUNTS: no cleaned row left pending', n = 0, 'pending rows: ' || n FROM pending
UNION ALL SELECT 5, 'RECORD COUNTS: fact_orders = source_registry', facts = registry, 'fact_orders=' || facts || ' registry=' || registry FROM reg
UNION ALL SELECT 6, 'RECORD COUNTS: loaded totals', true, 'orders=' || orders || ' lines=' || lines || ' customers=' || customers || ' products=' || products || ' rejected_rows(history)=' || rejected_rows FROM counts
UNION ALL SELECT 7, 'DUPLICATES: no duplicate order_id / customer email / sku', n = 0, 'duplicate keys: ' || n FROM dup_keys
UNION ALL SELECT 8, 'DUPLICATES: same content never loaded twice', n = 0, 'order+hash loaded more than once: ' || n FROM double_load
UNION ALL SELECT 9, 'DUPLICATES: dw hash = registry hash', n = 0, 'mismatches: ' || n FROM hash_mismatch
UNION ALL SELECT 10, 'MISSING VALUES: required fact columns not null', n = 0, 'null violations: ' || n FROM nulls_req
UNION ALL SELECT 11, 'MISSING VALUES: dimension keys / item values not null', n = 0, 'null violations: ' || n FROM nulls_dim
UNION ALL SELECT 12, 'MISSING VALUES: optional attributes (reported, imputed)', true,
       'customers w/o name=' || cust_no_name || ', w/o country=' || cust_no_country || ', products w/o name=' || prod_no_name
       || ', orders channel=unknown ' || orders_channel_unknown || ', status=unknown ' || orders_status_unknown FROM optional
UNION ALL SELECT 13, 'INTEGRITY: order_total = sum(lines), item_count = #lines', n = 0, 'mismatches: ' || n FROM total_mismatch
UNION ALL SELECT 14, 'LOADS: no run or batch stuck in running', n = 0, 'stuck: ' || n FROM stuck
UNION ALL SELECT 15, 'LOADS: no unfinished checkpoint (nothing left to resume)', n = 0, 'in_progress checkpoints: ' || n FROM open_cp
UNION ALL SELECT 16, 'LOADS: failed runs on record (history, informational)', true, 'failed runs: ' || n FROM failed_runs
ORDER BY ord;
