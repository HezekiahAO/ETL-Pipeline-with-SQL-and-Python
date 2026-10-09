-- =====================================================================
-- 01_schema.sql  -  database structure for every pipeline layer
--
--   raw          the XML file exactly as received (lineage / replay)
--   staging      fields extracted from the XML, still all TEXT
--   cleaned      typed + standardised + validated + de-duplicated
--   transformed  analysis-shaped rows (derived columns), one set per batch
--   dw           final star schema (dimensions + facts)   <- "PostgreSQL"
--   analysis     views that answer business questions
--   etl          pipeline metadata, checkpoint, watermark, dedup registry
--
-- Idempotent: safe to run repeatedly (CREATE ... IF NOT EXISTS).
-- =====================================================================
CREATE SCHEMA IF NOT EXISTS etl;
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS cleaned;
CREATE SCHEMA IF NOT EXISTS transformed;
CREATE SCHEMA IF NOT EXISTS dw;
CREATE SCHEMA IF NOT EXISTS analysis;

-- ========================= PIPELINE METADATA ==========================
-- One row per pipeline execution.
CREATE TABLE IF NOT EXISTS etl.pipeline_runs (
    run_id                   BIGSERIAL PRIMARY KEY,
    pipeline_name            TEXT        NOT NULL,
    run_mode                 TEXT        NOT NULL CHECK (run_mode IN ('full','incremental')),
    source_file              TEXT        NOT NULL,
    file_sha256              TEXT        NOT NULL,
    batch_size               INT         NOT NULL CHECK (batch_size > 0),
    started_at               TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    ended_at                 TIMESTAMPTZ,
    status                   TEXT        NOT NULL DEFAULT 'running'
                                         CHECK (status IN ('running','success','failed')),
    records_extracted        INT         NOT NULL DEFAULT 0,  -- <order> elements read from the XML
    records_skipped_watermark INT        NOT NULL DEFAULT 0,  -- incremental: older than the watermark
    records_skipped_checkpoint INT       NOT NULL DEFAULT 0,  -- resume: already done by an earlier run
    records_staged           INT         NOT NULL DEFAULT 0,  -- actually pushed into staging
    records_inserted         INT         NOT NULL DEFAULT 0,  -- new orders loaded
    records_updated          INT         NOT NULL DEFAULT 0,  -- changed orders re-loaded
    records_loaded           INT         GENERATED ALWAYS AS (records_inserted + records_updated) STORED,
    records_rejected         INT         NOT NULL DEFAULT 0,  -- failed validation
    records_duplicate        INT         NOT NULL DEFAULT 0,  -- same content already loaded / repeated in batch
    records_stale            INT         NOT NULL DEFAULT 0,  -- older version than what is loaded
    batches_completed        INT         NOT NULL DEFAULT 0,
    resumed_from_run_id      BIGINT,                           -- set when this run resumed a failed one
    resumed_after_position   INT,
    error_message            TEXT,
    error_batch_no           INT,
    executed_by              TEXT        NOT NULL DEFAULT current_user
);

-- One row per batch (a batch = one transaction).
CREATE TABLE IF NOT EXISTS etl.batch_log (
    batch_id        BIGSERIAL PRIMARY KEY,
    run_id          BIGINT      NOT NULL REFERENCES etl.pipeline_runs,
    batch_no        INT         NOT NULL,
    first_position  INT         NOT NULL,
    last_position   INT         NOT NULL,
    status          TEXT        NOT NULL DEFAULT 'running' CHECK (status IN ('running','success','failed')),
    rows_in         INT         NOT NULL DEFAULT 0,
    rows_inserted   INT         NOT NULL DEFAULT 0,
    rows_updated    INT         NOT NULL DEFAULT 0,
    rows_rejected   INT         NOT NULL DEFAULT 0,
    rows_duplicate  INT         NOT NULL DEFAULT 0,
    rows_stale      INT         NOT NULL DEFAULT 0,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    ended_at        TIMESTAMPTZ,
    error_message   TEXT,
    UNIQUE (run_id, batch_no)
);

-- CHECKPOINT: last position safely committed for a given source file.
-- Updated inside the same transaction as the batch => it can never run ahead of the data.
CREATE TABLE IF NOT EXISTS etl.checkpoint (
    pipeline_name   TEXT        NOT NULL,
    file_sha256     TEXT        NOT NULL,
    last_batch_no   INT         NOT NULL DEFAULT 0,
    last_position  INT          NOT NULL DEFAULT 0,
    status          TEXT        NOT NULL DEFAULT 'in_progress' CHECK (status IN ('in_progress','completed','abandoned')),
    run_id          BIGINT      REFERENCES etl.pipeline_runs,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline_name, file_sha256)
);

-- WATERMARK: highest source "modified" timestamp fully processed. Drives incremental loads.
CREATE TABLE IF NOT EXISTS etl.watermark (
    pipeline_name   TEXT PRIMARY KEY,
    high_watermark  TIMESTAMPTZ NOT NULL,
    run_id          BIGINT REFERENCES etl.pipeline_runs,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- DEDUP REGISTRY: what we already loaded for each source key, and its content hash.
CREATE TABLE IF NOT EXISTS etl.source_registry (
    source_key       TEXT PRIMARY KEY,                -- cleaned order_id
    record_hash      TEXT        NOT NULL,            -- sha256 of the cleaned business content
    source_modified  TIMESTAMPTZ,
    version_no       INT         NOT NULL DEFAULT 1,
    first_run_id     BIGINT      NOT NULL REFERENCES etl.pipeline_runs,
    last_run_id      BIGINT      NOT NULL REFERENCES etl.pipeline_runs,
    last_loaded_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Reference / lookup data (loaded from reference/*.csv)
CREATE TABLE IF NOT EXISTS etl.country_map (alias TEXT PRIMARY KEY, iso2   CHAR(2) NOT NULL);
CREATE TABLE IF NOT EXISTS etl.status_map  (alias TEXT PRIMARY KEY, status TEXT     NOT NULL);
CREATE TABLE IF NOT EXISTS etl.channel_map (alias TEXT PRIMARY KEY, channel TEXT    NOT NULL);

-- ================================ RAW =================================
CREATE TABLE IF NOT EXISTS raw.source_files (
    file_id       BIGSERIAL PRIMARY KEY,
    file_name     TEXT        NOT NULL,
    sha256        TEXT        NOT NULL UNIQUE,        -- identical file => identical row
    size_bytes    BIGINT      NOT NULL,
    record_count  INT,
    raw_content   TEXT        NOT NULL,               -- the untouched document (TEXT, so even malformed XML is archived)
    loaded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ============================== STAGING ===============================
-- Exactly what was extracted from the XML. Everything is TEXT: no judgement yet.
CREATE TABLE IF NOT EXISTS staging.orders_stg (
    stg_id                BIGSERIAL PRIMARY KEY,
    run_id                BIGINT NOT NULL REFERENCES etl.pipeline_runs,
    batch_id              BIGINT NOT NULL REFERENCES etl.batch_log,
    file_id               BIGINT NOT NULL REFERENCES raw.source_files,
    position              INT    NOT NULL,            -- 1-based position of <order> in the file
    order_id_raw          TEXT,
    modified_raw          TEXT,
    customer_name_raw     TEXT,
    customer_email_raw    TEXT,
    customer_country_raw  TEXT,
    order_date_raw        TEXT,
    status_raw            TEXT,
    channel_raw           TEXT,
    staged_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_stg_batch ON staging.orders_stg (batch_id);

CREATE TABLE IF NOT EXISTS staging.order_items_stg (
    stg_id     BIGINT NOT NULL REFERENCES staging.orders_stg ON DELETE CASCADE,
    line_no    INT    NOT NULL,
    sku_raw    TEXT,
    name_raw   TEXT,
    qty_raw    TEXT,
    price_raw  TEXT,
    PRIMARY KEY (stg_id, line_no)
);

-- ============================== CLEANED ===============================
-- Typed, standardised, validated. Every staged record gets exactly one row
-- here, labelled with what happened to it (dq_status / dq_reason).
CREATE TABLE IF NOT EXISTS cleaned.orders (
    clean_id          BIGSERIAL PRIMARY KEY,
    stg_id            BIGINT NOT NULL UNIQUE,
    run_id            BIGINT NOT NULL,
    batch_id          BIGINT NOT NULL,
    file_id           BIGINT NOT NULL,
    position          INT    NOT NULL,
    order_id          TEXT,
    modified_ts       TIMESTAMPTZ,
    customer_email    TEXT,
    customer_name     TEXT,
    customer_country  CHAR(2),
    order_date        DATE,
    status            TEXT,
    channel           TEXT,
    n_items           INT,
    n_bad_items       INT,
    order_total       NUMERIC(18,2),
    record_hash       TEXT,
    dq_status         TEXT NOT NULL DEFAULT 'pending' CHECK (dq_status IN
                        ('pending','valid','rejected','duplicate_in_batch',
                         'duplicate_already_loaded','stale_version')),
    dq_reason         TEXT,
    load_action       TEXT CHECK (load_action IN ('insert','update')),
    cleaned_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_cleaned_batch  ON cleaned.orders (batch_id);
CREATE INDEX IF NOT EXISTS ix_cleaned_order  ON cleaned.orders (order_id);

CREATE TABLE IF NOT EXISTS cleaned.order_items (
    clean_id      BIGINT NOT NULL REFERENCES cleaned.orders ON DELETE CASCADE,
    line_no       INT    NOT NULL,
    sku           TEXT,
    product_name  TEXT,
    qty           INT,
    unit_price    NUMERIC(12,2),
    is_valid      BOOLEAN NOT NULL,
    PRIMARY KEY (clean_id, line_no)
);

-- ============================ TRANSFORMED =============================
-- Only records that will be loaded (new or changed), reshaped for analysis.
CREATE TABLE IF NOT EXISTS transformed.orders_t (
    batch_id          BIGINT NOT NULL,
    order_id          TEXT   NOT NULL,
    customer_email    TEXT   NOT NULL,
    customer_name     TEXT,
    customer_country  CHAR(2),
    order_date        DATE   NOT NULL,
    order_year        INT    NOT NULL,
    order_quarter     INT    NOT NULL,
    order_month       INT    NOT NULL,
    status            TEXT   NOT NULL,
    channel           TEXT   NOT NULL,
    item_count        INT    NOT NULL,
    order_total       NUMERIC(18,2) NOT NULL,
    order_size_band   TEXT   NOT NULL CHECK (order_size_band IN ('small','medium','large')),
    record_hash       TEXT   NOT NULL,
    source_modified   TIMESTAMPTZ,
    load_action       TEXT   NOT NULL CHECK (load_action IN ('insert','update')),
    PRIMARY KEY (batch_id, order_id)
);
CREATE TABLE IF NOT EXISTS transformed.order_lines_t (
    batch_id      BIGINT NOT NULL,
    order_id      TEXT   NOT NULL,
    line_no       INT    NOT NULL,
    sku           TEXT   NOT NULL,
    product_name  TEXT,
    qty           INT    NOT NULL,
    unit_price    NUMERIC(12,2) NOT NULL,
    line_total    NUMERIC(18,2) NOT NULL,
    PRIMARY KEY (batch_id, order_id, line_no)
);

-- ================================ DW ==================================
CREATE TABLE IF NOT EXISTS dw.dim_date (
    date_key     INT  PRIMARY KEY,                -- yyyymmdd
    full_date    DATE NOT NULL UNIQUE,
    year         INT  NOT NULL,
    quarter      INT  NOT NULL,
    month        INT  NOT NULL,
    month_name   TEXT NOT NULL,
    day_of_week  INT  NOT NULL,                   -- 1 = Monday
    day_name     TEXT NOT NULL,
    is_weekend   BOOLEAN NOT NULL
);
INSERT INTO dw.dim_date
SELECT to_char(d,'YYYYMMDD')::int, d::date, extract(year FROM d)::int, extract(quarter FROM d)::int,
       extract(month FROM d)::int, trim(to_char(d,'Month')), extract(isodow FROM d)::int,
       trim(to_char(d,'Day')), extract(isodow FROM d) >= 6
FROM generate_series(DATE '2000-01-01', DATE '2050-12-31', INTERVAL '1 day') d
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS dw.dim_customer (
    customer_key     SERIAL PRIMARY KEY,
    email            TEXT NOT NULL UNIQUE CHECK (email = lower(email)),
    name             TEXT,
    country          CHAR(2),
    first_order_date DATE,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS dw.dim_product (
    product_key   SERIAL PRIMARY KEY,
    sku           TEXT NOT NULL UNIQUE,
    product_name  TEXT,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS dw.fact_orders (
    order_id              TEXT PRIMARY KEY,
    customer_key          INT  NOT NULL REFERENCES dw.dim_customer,
    date_key              INT  NOT NULL REFERENCES dw.dim_date,
    status                TEXT NOT NULL CHECK (status IN
                            ('pending','processing','shipped','delivered','cancelled','unknown')),
    channel               TEXT NOT NULL CHECK (channel IN ('web','mobile','store','phone','unknown')),
    item_count            INT  NOT NULL CHECK (item_count > 0),
    order_total           NUMERIC(18,2) NOT NULL CHECK (order_total >= 0),
    order_size_band       TEXT NOT NULL,
    record_hash           TEXT NOT NULL,
    source_modified       TIMESTAMPTZ,
    first_loaded_run_id   BIGINT NOT NULL REFERENCES etl.pipeline_runs,
    last_loaded_run_id    BIGINT NOT NULL REFERENCES etl.pipeline_runs,
    loaded_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_fact_orders_customer ON dw.fact_orders (customer_key);
CREATE INDEX IF NOT EXISTS ix_fact_orders_date     ON dw.fact_orders (date_key);

CREATE TABLE IF NOT EXISTS dw.fact_order_items (
    order_id     TEXT NOT NULL REFERENCES dw.fact_orders ON DELETE CASCADE,
    line_no      INT  NOT NULL,
    product_key  INT  NOT NULL REFERENCES dw.dim_product,
    qty          INT  NOT NULL CHECK (qty > 0),
    unit_price   NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0),
    line_total   NUMERIC(18,2) GENERATED ALWAYS AS (qty * unit_price) STORED,
    PRIMARY KEY (order_id, line_no)
);
CREATE INDEX IF NOT EXISTS ix_fact_items_product ON dw.fact_order_items (product_key);
