-- =====================================================================
-- 10_demo_sql_etl.sql - the whole ETL in PURE SQL (no Python)
--
--   Raw XML -> staging -> cleaned -> transformed -> dw -> analysis
--
-- PostgreSQL parses the XML itself with xpath(). Run via scripts/run_sql_demo.sh
--   SRC_FILE=data/raw_orders_full.xml        MODE=full        (first load)
--   SRC_FILE=data/raw_orders_incremental.xml MODE=incremental (second load)
-- =====================================================================
\set ON_ERROR_STOP on
\set src   `printf %s "${SRC_FILE:-data/raw_orders_full.xml}"`
\set mode  `printf %s "${MODE:-full}"`
\set xml   `cat "${SRC_FILE:-data/raw_orders_full.xml}"`
\set batch_size 250
\set pipeline orders_sql_demo

\echo
\echo '========== STEP 1  RAW: archive the XML document exactly as received =========='
INSERT INTO raw.source_files (file_name, sha256, size_bytes, raw_content)
VALUES (:'src', encode(sha256(convert_to(:'xml', 'UTF8')), 'hex'), octet_length(convert_to(:'xml', 'UTF8')), :'xml')
ON CONFLICT (sha256) DO UPDATE SET file_name = EXCLUDED.file_name      -- same file twice => same row
RETURNING file_id \gset
SELECT cardinality(xpath('/orders/order', raw_content::xml)) AS n_orders FROM raw.source_files WHERE file_id = :file_id \gset
UPDATE raw.source_files SET record_count = :n_orders WHERE file_id = :file_id;
SELECT file_id, file_name, size_bytes, record_count, left(sha256, 12) AS sha256 FROM raw.source_files WHERE file_id = :file_id;

\echo
\echo '========== STEP 2  open a run, plan the batches =========='
SELECT etl.start_run(:'pipeline', :'mode', :'src', (SELECT sha256 FROM raw.source_files WHERE file_id = :file_id), :batch_size) AS run_id \gset
INSERT INTO etl.batch_log (run_id, batch_no, first_position, last_position)
SELECT :run_id, g, (g - 1) * :batch_size + 1, least(g * :batch_size, :n_orders)
FROM generate_series(1, ceil(:n_orders::numeric / :batch_size)::int) g;
SELECT run_id, batch_no, first_position, last_position FROM etl.batch_log WHERE run_id = :run_id ORDER BY batch_no;

\echo
\echo '========== STEP 3  EXTRACT: xpath() turns each <order> into a row =========='
CREATE TEMP TABLE _order_nodes AS
SELECT n.pos::int AS pos, n.node
FROM raw.source_files f
CROSS JOIN LATERAL unnest(xpath('/orders/order', f.raw_content::xml)) WITH ORDINALITY AS n(node, pos)
WHERE f.file_id = :file_id;

-- incremental mode: drop what the watermark says we have already processed
SELECT coalesce((SELECT high_watermark FROM etl.watermark WHERE pipeline_name = :'pipeline'), '-infinity') AS wm \gset
WITH gone AS (
    DELETE FROM _order_nodes
    WHERE :'mode' = 'incremental'
      AND etl.clean_ts(etl.xml_text(xpath('string(/order/@modified)', node))) < :'wm'::timestamptz
    RETURNING 1)
SELECT count(*) AS skipped_wm FROM gone \gset
\echo watermark = :wm ; records skipped by watermark = :skipped_wm

INSERT INTO staging.orders_stg (run_id, batch_id, file_id, position, order_id_raw, modified_raw, customer_name_raw,
                                customer_email_raw, customer_country_raw, order_date_raw, status_raw, channel_raw)
SELECT :run_id, b.batch_id, :file_id, n.pos,
       etl.xml_text(xpath('string(/order/@id)',               n.node)),
       etl.xml_text(xpath('string(/order/@modified)',         n.node)),
       etl.xml_text(xpath('string(/order/customer/name)',     n.node)),
       etl.xml_text(xpath('string(/order/customer/email)',    n.node)),
       etl.xml_text(xpath('string(/order/customer/country)',  n.node)),
       etl.xml_text(xpath('string(/order/order_date)',        n.node)),
       etl.xml_text(xpath('string(/order/status)',            n.node)),
       etl.xml_text(xpath('string(/order/channel)',           n.node))
FROM _order_nodes n
JOIN etl.batch_log b ON b.run_id = :run_id AND n.pos BETWEEN b.first_position AND b.last_position;

INSERT INTO staging.order_items_stg (stg_id, line_no, sku_raw, name_raw, qty_raw, price_raw)
SELECT s.stg_id, it.ln::int,
       etl.xml_text(xpath('string(/item/@sku)',  it.node)), etl.xml_text(xpath('string(/item/name)',  it.node)),
       etl.xml_text(xpath('string(/item/@qty)',  it.node)), etl.xml_text(xpath('string(/item/price)', it.node))
FROM staging.orders_stg s
JOIN _order_nodes n ON n.pos = s.position
CROSS JOIN LATERAL unnest(xpath('/order/items/item', n.node)) WITH ORDINALITY AS it(node, ln)
WHERE s.run_id = :run_id;

\echo '--- staging.orders_stg: raw, messy, all TEXT (first 6 rows) ---'
SELECT position, order_id_raw, order_date_raw, status_raw, channel_raw, customer_country_raw, customer_email_raw
FROM staging.orders_stg WHERE run_id = :run_id ORDER BY position LIMIT 6;

SELECT batch_id AS b1 FROM etl.batch_log WHERE run_id = :run_id AND batch_no = 1 \gset

\echo
\echo '========== STEP 4  CLEAN (batch 1 walk-through): staging -> cleaned =========='
SELECT etl.clean_batch(:b1) AS rows_cleaned;
\echo '--- the same rows after cleaning: typed, standardised, validated ---'
SELECT position, order_id, order_date, status, channel, customer_country AS ctry, customer_email, order_total, dq_status, dq_reason
FROM cleaned.orders WHERE batch_id = :b1 ORDER BY position LIMIT 6;
\echo '--- rejected in batch 1 and why ---'
SELECT position, coalesce(order_id, '(none)') AS order_id, dq_reason FROM cleaned.orders WHERE batch_id = :b1 AND dq_status = 'rejected' ORDER BY position;

\echo
\echo '========== STEP 5  DEDUPLICATE (batch 1) =========='
SELECT etl.dedupe_batch(:b1);
SELECT dq_status, coalesce(dq_reason, '-') AS reason, coalesce(load_action, '-') AS action, count(*) AS records
FROM cleaned.orders WHERE batch_id = :b1 GROUP BY 1, 2, 3 ORDER BY 1, 2;

\echo
\echo '========== STEP 6  TRANSFORM (batch 1): cleaned -> transformed =========='
SELECT etl.transform_batch(:b1) AS orders_transformed;
SELECT order_id, order_date, order_year, order_quarter, order_month, item_count, order_total, order_size_band, load_action
FROM transformed.orders_t WHERE batch_id = :b1 ORDER BY order_id LIMIT 5;

\echo
\echo '========== STEP 7  LOAD (batch 1): transformed -> PostgreSQL dw =========='
SELECT etl.load_batch(:b1);
SELECT etl.close_batch(:b1) AS batch_1_result;

\echo
\echo '========== STEP 8  remaining batches: etl.process_batch() = clean + dedupe + transform + load =========='
SELECT format('SELECT %s AS batch_no, etl.process_batch(%s) AS result', batch_no, batch_id)
FROM etl.batch_log WHERE run_id = :run_id AND batch_no > 1 ORDER BY batch_no
\gexec

\echo
\echo '========== STEP 9  close the run, advance the watermark =========='
SELECT etl.advance_watermark(:'pipeline', max(modified_ts), :run_id) FROM cleaned.orders WHERE run_id = :run_id AND modified_ts IS NOT NULL;
SELECT etl.finish_run(:run_id, 'success', :n_orders, :skipped_wm, 0);
\i sql/22_metadata_queries.sql

\echo
\echo '========== STEP 10  VALIDATE =========='
\i sql/20_validation.sql

\echo
\echo '========== STEP 11  ANALYSIS =========='
\i sql/24_analysis_queries.sql
