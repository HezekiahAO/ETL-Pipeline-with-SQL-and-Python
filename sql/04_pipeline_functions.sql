-- =====================================================================
-- 04_pipeline_functions.sql
-- The set-based ETL logic, one function per layer transition, plus the
-- metadata helpers. Python (or the pure-SQL demo) calls these per batch:
--
--   staging --clean_batch--> cleaned --dedupe_batch--> cleaned (labelled)
--           --transform_batch--> transformed --load_batch--> dw
--
-- All of them take a batch_id, so a batch is the unit of work, of
-- transactionality and of recovery.
-- =====================================================================

-- ------------------------- METADATA HELPERS --------------------------
CREATE OR REPLACE FUNCTION etl.start_run(
    p_pipeline text, p_mode text, p_source text, p_sha text, p_batch_size int,
    p_resumed_from bigint DEFAULT NULL, p_resumed_after int DEFAULT NULL) RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE v_run bigint;
BEGIN
    -- A run still marked 'running' means its process died without cleaning up.
    UPDATE etl.pipeline_runs
       SET status = 'failed', ended_at = clock_timestamp(),
           error_message = coalesce(error_message, 'abandoned: process died before finishing')
     WHERE pipeline_name = p_pipeline AND status = 'running';

    INSERT INTO etl.pipeline_runs (pipeline_name, run_mode, source_file, file_sha256, batch_size,
                                   resumed_from_run_id, resumed_after_position)
    VALUES (p_pipeline, p_mode, p_source, p_sha, p_batch_size, p_resumed_from, p_resumed_after)
    RETURNING run_id INTO v_run;
    RETURN v_run;
END $$;

-- Roll batch counters up into the run row and close it.
CREATE OR REPLACE FUNCTION etl.finish_run(
    p_run bigint, p_status text, p_extracted int, p_skipped_wm int, p_skipped_cp int,
    p_error text DEFAULT NULL, p_error_batch int DEFAULT NULL) RETURNS void
LANGUAGE sql AS $$
    UPDATE etl.pipeline_runs r SET
        status = p_status, ended_at = clock_timestamp(),
        records_extracted = p_extracted, records_skipped_watermark = p_skipped_wm,
        records_skipped_checkpoint = p_skipped_cp,
        records_staged     = coalesce(b.rows_in, 0),
        records_inserted   = coalesce(b.ins, 0),  records_updated  = coalesce(b.upd, 0),
        records_rejected   = coalesce(b.rej, 0),  records_duplicate = coalesce(b.dup, 0),
        records_stale      = coalesce(b.stale, 0),
        batches_completed  = coalesce(b.n, 0),
        error_message = p_error, error_batch_no = p_error_batch
    FROM (SELECT p_run AS run_id) x
    LEFT JOIN (SELECT run_id, count(*) AS n, sum(rows_in) AS rows_in, sum(rows_inserted) AS ins,
                      sum(rows_updated) AS upd, sum(rows_rejected) AS rej,
                      sum(rows_duplicate) AS dup, sum(rows_stale) AS stale
               FROM etl.batch_log WHERE run_id = p_run AND status = 'success' GROUP BY run_id) b
           ON b.run_id = x.run_id
    WHERE r.run_id = p_run
$$;

-- A failed batch is rolled back, so its batch_log row is written afterwards, in a new transaction.
CREATE OR REPLACE FUNCTION etl.log_batch_failure(
    p_run bigint, p_batch_no int, p_first int, p_last int, p_rows int, p_error text) RETURNS void
LANGUAGE sql AS $$
    INSERT INTO etl.batch_log (run_id, batch_no, first_position, last_position, status, rows_in, ended_at, error_message)
    VALUES (p_run, p_batch_no, p_first, p_last, 'failed', p_rows, clock_timestamp(), p_error)
    ON CONFLICT (run_id, batch_no) DO UPDATE SET status = 'failed', error_message = EXCLUDED.error_message,
                                                 ended_at = clock_timestamp()
$$;

CREATE OR REPLACE FUNCTION etl.save_checkpoint(
    p_pipeline text, p_sha text, p_batch_no int, p_last_position int, p_run bigint) RETURNS void
LANGUAGE sql AS $$
    INSERT INTO etl.checkpoint (pipeline_name, file_sha256, last_batch_no, last_position, status, run_id)
    VALUES (p_pipeline, p_sha, p_batch_no, p_last_position, 'in_progress', p_run)
    ON CONFLICT (pipeline_name, file_sha256) DO UPDATE
       SET last_batch_no = EXCLUDED.last_batch_no, last_position = EXCLUDED.last_position,
           status = 'in_progress', run_id = EXCLUDED.run_id, updated_at = now()
$$;

-- Operator decision: "this unfinished file will never be resumed" (e.g. a corrupt file was replaced).
CREATE OR REPLACE FUNCTION etl.abandon_checkpoint(p_pipeline text, p_sha text) RETURNS int
LANGUAGE plpgsql AS $$
DECLARE n int;
BEGIN
    UPDATE etl.checkpoint SET status = 'abandoned', updated_at = now()
    WHERE pipeline_name = p_pipeline AND file_sha256 = p_sha AND status = 'in_progress';
    GET DIAGNOSTICS n = ROW_COUNT;
    RETURN n;
END $$;

CREATE OR REPLACE FUNCTION etl.advance_watermark(p_pipeline text, p_ts timestamptz, p_run bigint) RETURNS void
LANGUAGE sql AS $$
    INSERT INTO etl.watermark (pipeline_name, high_watermark, run_id) VALUES (p_pipeline, p_ts, p_run)
    ON CONFLICT (pipeline_name) DO UPDATE
       SET high_watermark = GREATEST(etl.watermark.high_watermark, EXCLUDED.high_watermark),
           run_id = EXCLUDED.run_id, updated_at = now()
$$;

-- ------------------------ 1. STAGING -> CLEANED ----------------------
CREATE OR REPLACE FUNCTION etl.clean_batch(p_batch bigint) RETURNS int
LANGUAGE plpgsql AS $$
DECLARE n int;
BEGIN
    -- standardise formats (dates, e-mail, names, countries, status, channel, ids)
    INSERT INTO cleaned.orders (stg_id, run_id, batch_id, file_id, position, order_id, modified_ts,
                                customer_email, customer_name, customer_country, order_date,
                                status, channel)
    SELECT s.stg_id, s.run_id, s.batch_id, s.file_id, s.position,
           etl.clean_order_id(s.order_id_raw),  etl.clean_ts(s.modified_raw),
           etl.clean_email(s.customer_email_raw), etl.proper(s.customer_name_raw),
           etl.norm_country(s.customer_country_raw), etl.clean_date(s.order_date_raw),
           etl.norm_status(s.status_raw),        etl.norm_channel(s.channel_raw)
    FROM staging.orders_stg s
    WHERE s.batch_id = p_batch;
    GET DIAGNOSTICS n = ROW_COUNT;

    -- order lines: clean + flag invalid ones
    INSERT INTO cleaned.order_items (clean_id, line_no, sku, product_name, qty, unit_price, is_valid)
    SELECT c.clean_id, i.line_no, x.sku, x.pname, x.qty, x.price,
           coalesce(x.sku ~ '^[A-Z0-9][A-Z0-9_-]*$'
                    AND x.qty   BETWEEN 1 AND 100000
                    AND x.price BETWEEN 0 AND 10000000, false)
    FROM staging.order_items_stg i
    JOIN cleaned.orders c ON c.stg_id = i.stg_id
    CROSS JOIN LATERAL (SELECT upper(etl.squish(i.sku_raw))           AS sku,
                               etl.squish(i.name_raw)                 AS pname,
                               etl.clean_qty(i.qty_raw)               AS qty,
                               round(etl.clean_money(i.price_raw), 2) AS price) x
    WHERE c.batch_id = p_batch;

    -- roll lines up to the order: totals + content hash (the "same record?" fingerprint)
    UPDATE cleaned.orders c SET
        n_items     = coalesce(a.n_items, 0),
        n_bad_items = coalesce(a.n_bad, 0),
        order_total = a.total,
        record_hash = encode(sha256(convert_to(
            format('%s|%s|%s|%s|%s|%s|%s|%s', c.order_id, c.customer_email, c.customer_name,
                   c.customer_country, c.order_date, c.status, c.channel, a.sig), 'UTF8')), 'hex')
    FROM (SELECT o.clean_id, g.n_items, g.n_bad, g.total, g.sig
          FROM cleaned.orders o
          LEFT JOIN (SELECT clean_id, count(*) AS n_items,
                            count(*) FILTER (WHERE NOT is_valid) AS n_bad,
                            sum(qty * unit_price) FILTER (WHERE is_valid) AS total,
                            string_agg(format('%s:%s:%s:%s', line_no, sku, qty, unit_price), ';' ORDER BY line_no) AS sig
                     FROM cleaned.order_items GROUP BY clean_id) g USING (clean_id)
          WHERE o.batch_id = p_batch) a
    WHERE c.clean_id = a.clean_id;

    -- validate: first failing rule wins. NULL reason = valid.
    UPDATE cleaned.orders SET
        dq_reason = CASE WHEN order_id       IS NULL THEN 'missing_order_id'
                         WHEN customer_email IS NULL THEN 'invalid_or_missing_email'
                         WHEN order_date     IS NULL THEN 'invalid_or_missing_date'
                         WHEN n_items = 0            THEN 'no_items'
                         WHEN n_bad_items > 0        THEN 'invalid_item' END
    WHERE batch_id = p_batch;
    UPDATE cleaned.orders SET dq_status = CASE WHEN dq_reason IS NULL THEN 'valid' ELSE 'rejected' END
    WHERE batch_id = p_batch;
    RETURN n;
END $$;

-- ----------------------- 2. DEDUPLICATION ----------------------------
-- (a) inside the batch: same order_id repeated -> keep the newest (modified, then position)
-- (b) against history: compare with etl.source_registry
--       not seen before            -> insert
--       same hash                  -> duplicate_already_loaded   (skip)
--       different hash, newer      -> update
--       different hash, older      -> stale_version              (skip)
CREATE OR REPLACE FUNCTION etl.dedupe_batch(p_batch bigint) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    UPDATE cleaned.orders c SET
        dq_status = 'duplicate_in_batch',
        dq_reason = CASE WHEN c.record_hash = r.keep_hash THEN 'same_content_repeated'
                         ELSE 'superseded_by_newer_version' END
    FROM (SELECT clean_id,
                 row_number() OVER w AS rn,
                 first_value(record_hash) OVER w AS keep_hash
          FROM cleaned.orders
          WHERE batch_id = p_batch AND dq_status = 'valid'
          WINDOW w AS (PARTITION BY order_id ORDER BY modified_ts DESC NULLS LAST, position DESC)) r
    WHERE c.clean_id = r.clean_id AND r.rn > 1;

    UPDATE cleaned.orders c SET
        dq_status = CASE WHEN g.reg_hash IS NULL                  THEN 'valid'
                         WHEN g.reg_hash = c.record_hash           THEN 'duplicate_already_loaded'
                         WHEN coalesce(c.modified_ts, '-infinity') >= coalesce(g.reg_mod, '-infinity') THEN 'valid'
                         ELSE 'stale_version' END,
        dq_reason = CASE WHEN g.reg_hash = c.record_hash           THEN 'identical_to_loaded_version'
                         WHEN g.reg_hash IS NOT NULL
                          AND coalesce(c.modified_ts, '-infinity') < coalesce(g.reg_mod, '-infinity')
                                                                  THEN 'older_than_loaded_version' END,
        load_action = CASE WHEN g.reg_hash IS NULL THEN 'insert'
                           WHEN g.reg_hash <> c.record_hash
                            AND coalesce(c.modified_ts, '-infinity') >= coalesce(g.reg_mod, '-infinity') THEN 'update' END
    FROM (SELECT o.clean_id, r.record_hash AS reg_hash, r.source_modified AS reg_mod
          FROM cleaned.orders o
          LEFT JOIN etl.source_registry r ON r.source_key = o.order_id
          WHERE o.batch_id = p_batch AND o.dq_status = 'valid') g
    WHERE c.clean_id = g.clean_id;
END $$;

-- ----------------------- 3. CLEANED -> TRANSFORMED -------------------
CREATE OR REPLACE FUNCTION etl.transform_batch(p_batch bigint) RETURNS int
LANGUAGE plpgsql AS $$
DECLARE n int;
BEGIN
    INSERT INTO transformed.orders_t
    SELECT c.batch_id, c.order_id, c.customer_email, c.customer_name, c.customer_country,
           c.order_date,
           extract(year    FROM c.order_date)::int,
           extract(quarter FROM c.order_date)::int,
           extract(month   FROM c.order_date)::int,
           c.status, c.channel, c.n_items, c.order_total,
           CASE WHEN c.order_total >= 1000 THEN 'large'
                WHEN c.order_total >= 250  THEN 'medium' ELSE 'small' END,
           c.record_hash, c.modified_ts, c.load_action
    FROM cleaned.orders c
    WHERE c.batch_id = p_batch AND c.dq_status = 'valid';
    GET DIAGNOSTICS n = ROW_COUNT;

    INSERT INTO transformed.order_lines_t
    SELECT c.batch_id, c.order_id, i.line_no, i.sku, i.product_name, i.qty, i.unit_price, i.qty * i.unit_price
    FROM cleaned.order_items i
    JOIN cleaned.orders c USING (clean_id)
    WHERE c.batch_id = p_batch AND c.dq_status = 'valid';
    RETURN n;
END $$;

-- ----------------------- 4. TRANSFORMED -> DW ------------------------
-- Upserts only: running a batch twice leaves exactly the same rows.
CREATE OR REPLACE FUNCTION etl.load_batch(p_batch bigint) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE v_run bigint;
BEGIN
    SELECT run_id INTO v_run FROM etl.batch_log WHERE batch_id = p_batch;

    INSERT INTO dw.dim_product (sku, product_name)
    SELECT DISTINCT ON (l.sku) l.sku, l.product_name
    FROM transformed.order_lines_t l
    JOIN transformed.orders_t o USING (batch_id, order_id)
    WHERE l.batch_id = p_batch
    ORDER BY l.sku, (l.product_name IS NULL), o.source_modified DESC NULLS LAST, o.order_id DESC
    ON CONFLICT (sku) DO UPDATE
       SET product_name = COALESCE(EXCLUDED.product_name, dw.dim_product.product_name), updated_at = now()
     WHERE dw.dim_product.product_name IS DISTINCT FROM COALESCE(EXCLUDED.product_name, dw.dim_product.product_name);

    INSERT INTO dw.dim_customer (email, name, country)
    SELECT customer_email,
           (array_agg(customer_name    ORDER BY source_modified DESC NULLS LAST, order_id DESC)
                FILTER (WHERE customer_name    IS NOT NULL))[1],
           (array_agg(customer_country ORDER BY source_modified DESC NULLS LAST, order_id DESC)
                FILTER (WHERE customer_country IS NOT NULL))[1]
    FROM transformed.orders_t WHERE batch_id = p_batch
    GROUP BY customer_email
    ON CONFLICT (email) DO UPDATE
       SET name = COALESCE(EXCLUDED.name, dw.dim_customer.name),
           country = COALESCE(EXCLUDED.country, dw.dim_customer.country), updated_at = now()
     WHERE (dw.dim_customer.name, dw.dim_customer.country)
           IS DISTINCT FROM (COALESCE(EXCLUDED.name, dw.dim_customer.name),
                             COALESCE(EXCLUDED.country, dw.dim_customer.country));

    INSERT INTO dw.fact_orders (order_id, customer_key, date_key, status, channel, item_count, order_total,
                                order_size_band, record_hash, source_modified, first_loaded_run_id, last_loaded_run_id)
    SELECT o.order_id, c.customer_key, to_char(o.order_date, 'YYYYMMDD')::int, o.status, o.channel,
           o.item_count, o.order_total, o.order_size_band, o.record_hash, o.source_modified, v_run, v_run
    FROM transformed.orders_t o
    JOIN dw.dim_customer c ON c.email = o.customer_email
    WHERE o.batch_id = p_batch
    ON CONFLICT (order_id) DO UPDATE
       SET customer_key = EXCLUDED.customer_key, date_key = EXCLUDED.date_key, status = EXCLUDED.status,
           channel = EXCLUDED.channel, item_count = EXCLUDED.item_count, order_total = EXCLUDED.order_total,
           order_size_band = EXCLUDED.order_size_band, record_hash = EXCLUDED.record_hash,
           source_modified = EXCLUDED.source_modified, last_loaded_run_id = EXCLUDED.last_loaded_run_id,
           updated_at = now();

    DELETE FROM dw.fact_order_items WHERE order_id IN (SELECT order_id FROM transformed.orders_t WHERE batch_id = p_batch);
    INSERT INTO dw.fact_order_items (order_id, line_no, product_key, qty, unit_price)
    SELECT l.order_id, l.line_no, p.product_key, l.qty, l.unit_price
    FROM transformed.order_lines_t l
    JOIN dw.dim_product p ON p.sku = l.sku
    WHERE l.batch_id = p_batch;

    UPDATE dw.dim_customer c SET first_order_date = x.d
    FROM (SELECT f.customer_key, min(dd.full_date) AS d
          FROM dw.fact_orders f JOIN dw.dim_date dd USING (date_key)
          WHERE f.customer_key IN (SELECT dc.customer_key FROM dw.dim_customer dc
                                   WHERE dc.email IN (SELECT customer_email FROM transformed.orders_t WHERE batch_id = p_batch))
          GROUP BY f.customer_key) x
    WHERE c.customer_key = x.customer_key AND c.first_order_date IS DISTINCT FROM x.d;

    -- registry last: this is what makes a replayed record a recognised duplicate
    INSERT INTO etl.source_registry (source_key, record_hash, source_modified, first_run_id, last_run_id)
    SELECT order_id, record_hash, source_modified, v_run, v_run
    FROM transformed.orders_t WHERE batch_id = p_batch
    ON CONFLICT (source_key) DO UPDATE
       SET record_hash = EXCLUDED.record_hash, source_modified = EXCLUDED.source_modified,
           last_run_id = EXCLUDED.last_run_id, version_no = etl.source_registry.version_no + 1,
           last_loaded_at = now();
END $$;

-- ----------------------- batch bookkeeping + end to end --------------
-- Summarise what happened to the batch and close its batch_log row.
CREATE OR REPLACE FUNCTION etl.close_batch(p_batch bigint) RETURNS jsonb
LANGUAGE plpgsql AS $$
DECLARE s jsonb;
BEGIN
    SELECT jsonb_build_object(
        'rows_in',   count(*),
        'inserted',  count(*) FILTER (WHERE load_action = 'insert'),
        'updated',   count(*) FILTER (WHERE load_action = 'update'),
        'rejected',  count(*) FILTER (WHERE dq_status = 'rejected'),
        'duplicate', count(*) FILTER (WHERE dq_status IN ('duplicate_in_batch','duplicate_already_loaded')),
        'stale',     count(*) FILTER (WHERE dq_status = 'stale_version'))
    INTO s FROM cleaned.orders WHERE batch_id = p_batch;

    UPDATE etl.batch_log SET status = 'success', ended_at = clock_timestamp(),
           rows_in = (s->>'rows_in')::int,      rows_inserted = (s->>'inserted')::int,
           rows_updated = (s->>'updated')::int, rows_rejected = (s->>'rejected')::int,
           rows_duplicate = (s->>'duplicate')::int, rows_stale = (s->>'stale')::int
    WHERE batch_id = p_batch;
    RETURN s;
END $$;

CREATE OR REPLACE FUNCTION etl.process_batch(p_batch bigint) RETURNS jsonb
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM etl.clean_batch(p_batch);
    PERFORM etl.dedupe_batch(p_batch);
    PERFORM etl.transform_batch(p_batch);
    PERFORM etl.load_batch(p_batch);
    RETURN etl.close_batch(p_batch);
END $$;

-- Convenience view: everything that was NOT loaded, and why.
CREATE OR REPLACE VIEW etl.v_not_loaded AS
SELECT c.run_id, c.batch_id, c.position, c.order_id, c.dq_status, c.dq_reason, c.modified_ts
FROM cleaned.orders c WHERE c.dq_status <> 'valid';
