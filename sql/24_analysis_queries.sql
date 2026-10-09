-- =====================================================================
-- 24_analysis_queries.sql - the "Analysis" stage: questions answered from dw
-- =====================================================================
\echo '--- Monthly revenue (excl. cancelled) ---'
SELECT year, month, month_name, orders, revenue, avg_order_value FROM analysis.v_monthly_revenue ORDER BY year, month;

\echo '--- Revenue by country ---'
SELECT * FROM analysis.v_revenue_by_country ORDER BY revenue DESC;

\echo '--- Top 5 products by revenue ---'
SELECT * FROM analysis.v_top_products ORDER BY revenue DESC LIMIT 5;

\echo '--- Channel performance ---'
SELECT * FROM analysis.v_channel_performance ORDER BY revenue DESC;

\echo '--- Customer segments ---'
SELECT segment, count(*) AS customers, sum(lifetime_value) AS revenue, round(avg(orders),1) AS avg_orders
FROM analysis.v_customer_value GROUP BY segment ORDER BY revenue DESC;

\echo '--- Order status mix ---'
SELECT * FROM analysis.v_order_status_mix ORDER BY orders DESC;

\echo '--- Weekend vs weekday ---'
SELECT CASE WHEN d.is_weekend THEN 'weekend' ELSE 'weekday' END AS day_type, count(*) AS orders, sum(f.order_total) AS revenue
FROM dw.fact_orders f JOIN dw.dim_date d USING (date_key) WHERE f.status <> 'cancelled' GROUP BY 1 ORDER BY 1;
