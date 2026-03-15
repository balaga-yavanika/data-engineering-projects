-- Dimension table: product metadata enriched with performance metrics

WITH products AS (
    SELECT * FROM {{ ref('stg_products') }}
),
item_metrics AS (
    SELECT
        i.product_id,
        COUNT(DISTINCT i.order_id)          AS times_ordered,
        AVG(i.unit_price)                   AS avg_selling_price,
        SUM(i.unit_price)                   AS total_revenue,
        AVG(r.score)                 AS avg_review_score
    FROM {{ ref('stg_order_items') }}       i
    LEFT JOIN {{ ref('stg_orders') }}       o ON i.order_id = o.order_id
    LEFT JOIN {{ ref('stg_order_reviews') }} r ON i.order_id = r.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY i.product_id
)
SELECT
    p.product_id,
    p.category,
    p.weight_grams,
    p.photo_count,
    m.times_ordered,
    m.avg_selling_price,
    m.total_revenue,
    m.avg_review_score

FROM products      p
LEFT JOIN item_metrics m ON p.product_id = m.product_id
