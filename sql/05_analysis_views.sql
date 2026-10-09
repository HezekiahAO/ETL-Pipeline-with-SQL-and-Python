-- =====================================================================
-- 05_analysis_views.sql - the "Analysis" layer: business questions on dw
-- (cancelled orders are excluded from revenue)
-- =====================================================================
CREATE OR REPLACE VIEW analysis.v_monthly_revenue AS
SELECT d.year, d.month, d.month_name,
       count(*)                                   AS orders,
       sum(f.order_total)                         AS revenue,
       round(avg(f.order_total), 2)               AS avg_order_value
FROM dw.fact_orders f JOIN dw.dim_date d USING (date_key)
WHERE f.status <> 'cancelled'
GROUP BY d.year, d.month, d.month_name;

CREATE OR REPLACE VIEW analysis.v_revenue_by_country AS
SELECT coalesce(c.country, '??') AS country,
       count(DISTINCT c.customer_key) AS customers, count(*) AS orders, sum(f.order_total) AS revenue
FROM dw.fact_orders f JOIN dw.dim_customer c USING (customer_key)
WHERE f.status <> 'cancelled'
GROUP BY 1;

CREATE OR REPLACE VIEW analysis.v_top_products AS
SELECT p.sku, p.product_name, sum(i.qty) AS units_sold, sum(i.line_total) AS revenue,
       count(DISTINCT i.order_id) AS orders
FROM dw.fact_order_items i
JOIN dw.fact_orders f USING (order_id)
JOIN dw.dim_product p USING (product_key)
WHERE f.status <> 'cancelled'
GROUP BY p.sku, p.product_name;

CREATE OR REPLACE VIEW analysis.v_customer_value AS
SELECT c.customer_key, c.email, c.name, c.country, c.first_order_date,
       count(*) AS orders, sum(f.order_total) AS lifetime_value,
       CASE WHEN sum(f.order_total) >= 3000 THEN 'gold'
            WHEN sum(f.order_total) >= 1000 THEN 'silver' ELSE 'bronze' END AS segment
FROM dw.fact_orders f JOIN dw.dim_customer c USING (customer_key)
WHERE f.status <> 'cancelled'
GROUP BY c.customer_key, c.email, c.name, c.country, c.first_order_date;

CREATE OR REPLACE VIEW analysis.v_channel_performance AS
SELECT channel, count(*) AS orders, sum(order_total) AS revenue, round(avg(order_total), 2) AS avg_order_value
FROM dw.fact_orders WHERE status <> 'cancelled' GROUP BY channel;

CREATE OR REPLACE VIEW analysis.v_order_status_mix AS
SELECT status, count(*) AS orders, round(100.0 * count(*) / sum(count(*)) OVER (), 1) AS pct
FROM dw.fact_orders GROUP BY status;
