-- Fact table: one row per order, joining all relevant dimensions
-- This is the primary table analysts will query for revenue metrics

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),
items AS (
    SELECT
        order_id,
        COUNT(*)                      AS item_count,
        SUM(unit_price)               AS items_subtotal,
        SUM(freight_value)            AS total_freight
    FROM {{ ref('stg_order_items') }}
    GROUP BY order_id
),
payments AS (
    SELECT
        order_id,
        SUM(payment_amount)           AS total_payment,
        MAX(payment_type)             AS primary_payment_type,
        MAX(installments)             AS max_installments
    FROM {{ ref('stg_order_payments') }}
    GROUP BY order_id
),
reviews AS (
    SELECT order_id, score AS review_score
    FROM {{ ref('stg_order_reviews') }}
)

SELECT
    o.order_id,
    o.customer_id,
    o.order_status,
    o.ordered_at,
    o.approved_at,
    o.shipped_at,
    o.delivered_at,
    o.estimated_delivery_at,

    -- Delivery performance
    DATEDIFF('day', o.ordered_at, o.delivered_at)           AS days_to_deliver,
    DATEDIFF('day', o.delivered_at, o.estimated_delivery_at) AS days_early_late,
    CASE
        WHEN o.delivered_at <= o.estimated_delivery_at THEN 'on_time'
        WHEN o.delivered_at >  o.estimated_delivery_at THEN 'late'
        ELSE 'not_delivered'
    END AS delivery_status,

 -- Financial
    i.item_count,
    i.items_subtotal,
    i.total_freight,
    p.total_payment,
    p.primary_payment_type,
    p.max_installments,

    -- Customer feedback
    r.review_score

FROM orders          o
LEFT JOIN items      i ON o.order_id = i.order_id
LEFT JOIN payments   p ON o.order_id = p.order_id
LEFT JOIN reviews    r ON o.order_id = r.order_id
