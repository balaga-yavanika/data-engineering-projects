-- Dimension table: one row per unique customer with lifetime metrics
-- customer_unique_id deduplicates customers who placed multiple orders

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),
order_metrics AS (
    SELECT
        o.customer_id,
        COUNT(DISTINCT o.order_id)             AS total_orders,
        SUM(p.payment_amount)                  AS lifetime_value,
        MIN(o.ordered_at)                      AS first_order_at,
        MAX(o.ordered_at)                      AS last_order_at,
        AVG(r.score)                    AS avg_review_score
    FROM {{ ref('fct_orders') }}               o
    LEFT JOIN {{ ref('stg_order_payments') }}  p ON o.order_id = p.order_id
    LEFT JOIN {{ ref('stg_order_reviews') }}   r ON o.order_id = r.order_id
    GROUP BY o.customer_id
)

SELECT
    c.customer_id,
    c.customer_unique_id,
    c.city,
    c.state,
    c.zip_prefix,
    m.total_orders,
    m.lifetime_value,
    m.first_order_at,
    m.last_order_at,
    m.avg_review_score,

    -- Customer segment based on order frequency
    CASE
        WHEN m.total_orders = 1 THEN 'one_time'
        WHEN m.total_orders BETWEEN 2 AND 4 THEN 'repeat'
        WHEN m.total_orders >= 5 THEN 'loyal'
    END AS customer_segment

FROM customers     c
LEFT JOIN order_metrics m ON c.customer_id = m.customer_id
