-- Cohort = the month a customer placed their first order
-- Shows how many customers from each cohort ordered again in later months

WITH first_order AS (
    SELECT
        customer_unique_id,
        DATE_TRUNC('month', first_order_at)::DATE AS cohort_month
    FROM ecommerce_db.marts.dim_customers
),
customer_orders AS (
    SELECT
        c.customer_unique_id,
        DATE_TRUNC('month', o.ordered_at)::DATE  AS order_month
    FROM ecommerce_db.marts.fct_orders   o
    JOIN ecommerce_db.staging.stg_customers c ON o.customer_id = c.customer_id
)

SELECT
    f.cohort_month,
    DATEDIFF('month', f.cohort_month, co.order_month) AS months_since_first,
    COUNT(DISTINCT f.customer_unique_id)               AS cohort_size,
    COUNT(DISTINCT co.customer_unique_id)              AS active_customers,
    ROUND(COUNT(DISTINCT co.customer_unique_id) * 100.0 /
          COUNT(DISTINCT f.customer_unique_id), 1)    AS retention_pct
FROM first_order     f
JOIN customer_orders co ON f.customer_unique_id = co.customer_unique_id
GROUP BY 1, 2
ORDER BY 1, 2;
