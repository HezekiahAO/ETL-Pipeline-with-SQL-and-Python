
-- 03_cleaning_functions.sql - one small function per cleaning rule.
-- Each returns NULL when a value is missing or cannot be repaired; the
-- validation step (04_pipeline_functions.sql) decides what NULL means.


-- XML text extraction helper: xpath() returns xml[]; take first item,
-- decode entities (&amp; -> &), blank -> NULL.
CREATE OR REPLACE FUNCTION etl.xml_text(x xml[]) RETURNS text
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN s.t IS NULL OR btrim(s.t) = '' THEN NULL
                ELSE replace(replace(replace(replace(replace(s.t,'&lt;','<'),'&gt;','>'),'&quot;','"'),'&apos;',''''),'&amp;','&')
           END
    FROM (SELECT (x)[1]::text AS t) s
$$;

-- Trim, collapse inner whitespace, treat placeholder strings as missing.
CREATE OR REPLACE FUNCTION etl.squish(t text) RETURNS text
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN lower(s.x) IN ('', 'n/a', 'na', 'null', 'none', 'nil', '-') THEN NULL ELSE s.x END
    FROM (SELECT btrim(regexp_replace(coalesce(t, ''), '\s+', ' ', 'g')) AS x) s
$$;

-- "  alice   JOHNSON " -> "Alice Johnson"
CREATE OR REPLACE FUNCTION etl.proper(t text) RETURNS text
LANGUAGE sql IMMUTABLE AS $$ SELECT initcap(etl.squish(t)) $$;

-- "100001" -> "ORD-100001";  " ord-100001 " -> "ORD-100001";  garbage -> NULL
CREATE OR REPLACE FUNCTION etl.clean_order_id(t text) RETURNS text
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN s.x ~ '^[0-9]+$'             THEN 'ORD-' || s.x
                WHEN s.x ~ '^[A-Z0-9][A-Z0-9-]*$' THEN s.x END
    FROM (SELECT upper(etl.squish(t)) AS x) s
$$;

-- lower-case and shape check
CREATE OR REPLACE FUNCTION etl.clean_email(t text) RETURNS text
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN s.e ~ '^[a-z0-9._%+-]+@[a-z0-9-]+(?:\.[a-z0-9-]+)*\.[a-z]{2,}$' THEN s.e END
    FROM (SELECT lower(etl.squish(t)) AS e) s
$$;

-- "$1,299.50" -> 1299.50 ;  "free" -> NULL
CREATE OR REPLACE FUNCTION etl.clean_money(t text) RETURNS numeric
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN s.y ~ '^-?[0-9]+(?:\.[0-9]+)?$' THEN s.y::numeric END
    FROM (SELECT regexp_replace(coalesce(etl.squish(t), ''), '[$€£,\s]', '', 'g') AS y) s
$$;

-- "2" / "2.0" -> 2 ;  "-1" / "two" -> NULL
CREATE OR REPLACE FUNCTION etl.clean_qty(t text) RETURNS int
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN s.x ~ '^[0-9]{1,6}(?:\.0+)?$' THEN split_part(s.x, '.', 1)::int END
    FROM (SELECT etl.squish(t) AS x) s
$$;

-- ISO (optionally with time/zone), US MM/DD/YYYY, "17 Mar 2024", epoch seconds.
-- Impossible dates (2024-02-30) and years outside 2000-2050 -> NULL.
CREATE OR REPLACE FUNCTION etl.clean_date(t text) RETURNS date
LANGUAGE plpgsql AS $$
DECLARE
    x text := etl.squish(t);
    d date;
BEGIN
    IF x IS NULL THEN RETURN NULL; END IF;
    IF x ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}(?:[T ][0-9]{2}:[0-9]{2}(?::[0-9]{2}(?:\.[0-9]+)?)?(?:Z|[+-][0-9]{2}:?[0-9]{2})?)?$' THEN
        d := make_date(substr(x,1,4)::int, substr(x,6,2)::int, substr(x,9,2)::int);
    ELSIF x ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' THEN
        d := make_date(split_part(x,'/',3)::int, split_part(x,'/',1)::int, split_part(x,'/',2)::int);
    ELSIF x ~* '^[0-9]{1,2} [a-z]{3} [0-9]{4}$' THEN
        d := to_date(x, 'DD Mon YYYY');
    ELSIF x ~ '^[0-9]{9,10}$' THEN
        d := (to_timestamp(x::bigint) AT TIME ZONE 'UTC')::date;
    END IF;
    IF d < DATE '2000-01-01' OR d > DATE '2050-12-31' THEN RETURN NULL; END IF;
    RETURN d;
EXCEPTION WHEN others THEN
    RETURN NULL;            -- make_date / to_date raise on impossible dates
END $$;

-- Source "modified" timestamp (ISO-8601). No zone => assume UTC.
CREATE OR REPLACE FUNCTION etl.clean_ts(t text) RETURNS timestamptz
LANGUAGE plpgsql AS $$
DECLARE x text := etl.squish(t);
BEGIN
    IF x IS NULL THEN RETURN NULL; END IF;
    IF x ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}(?::[0-9]{2}(?:\.[0-9]+)?)?(?:Z|[+-][0-9]{2}:?[0-9]{2})$' THEN
        RETURN x::timestamptz;
    ELSIF x ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}(?::[0-9]{2}(?:\.[0-9]+)?)?$' THEN
        RETURN x::timestamp AT TIME ZONE 'UTC';
    END IF;
    RETURN NULL;
EXCEPTION WHEN others THEN
    RETURN NULL;
END $$;

-- Lookups: "U.S.A" -> 'US', " Canceled " -> 'cancelled', "In-Store" -> 'store'
CREATE OR REPLACE FUNCTION etl.norm_country(t text) RETURNS text
LANGUAGE sql STABLE AS $$
    SELECT m.iso2::text FROM etl.country_map m
    WHERE m.alias = regexp_replace(lower(coalesce(t, '')), '[^a-z]', '', 'g')
$$;
CREATE OR REPLACE FUNCTION etl.norm_status(t text) RETURNS text
LANGUAGE sql STABLE AS $$
    SELECT coalesce((SELECT s.status FROM etl.status_map s WHERE s.alias = lower(etl.squish(t))), 'unknown')
$$;
CREATE OR REPLACE FUNCTION etl.norm_channel(t text) RETURNS text
LANGUAGE sql STABLE AS $$
    SELECT coalesce((SELECT m.channel FROM etl.channel_map m
                     WHERE m.alias = regexp_replace(lower(coalesce(t, '')), '[^a-z]', '', 'g')), 'unknown')
$$;
