-- Content fingerprint of the warehouse (ignores surrogate keys and timestamps).
-- Two runs that produce the same fingerprint produced the same data.
SELECT 'fact_orders'      AS table_name, count(*) AS row_count,
       md5(string_agg(f.order_id||'|'||c.email||'|'||f.date_key||'|'||f.status||'|'||f.channel||'|'||f.order_total||'|'||f.record_hash, ',' ORDER BY f.order_id)) AS fingerprint
FROM dw.fact_orders f JOIN dw.dim_customer c USING (customer_key)
UNION ALL
SELECT 'fact_order_items', count(*),
       md5(string_agg(i.order_id||'|'||i.line_no||'|'||p.sku||'|'||i.qty||'|'||i.unit_price, ',' ORDER BY i.order_id, i.line_no))
FROM dw.fact_order_items i JOIN dw.dim_product p USING (product_key)
UNION ALL
SELECT 'dim_customer', count(*), md5(string_agg(email||'|'||coalesce(name,'')||'|'||coalesce(country,'')||'|'||coalesce(first_order_date::text,''), ',' ORDER BY email))
FROM dw.dim_customer
UNION ALL
SELECT 'dim_product', count(*), md5(string_agg(sku||'|'||coalesce(product_name,''), ',' ORDER BY sku))
FROM dw.dim_product;
