WITH source AS (
    SELECT * FROM {{ source('raw', 'orders') }}
),

renamed AS (
    SELECT
        order_id,
        customer_id,
        order_status,
        TRY_TO_TIMESTAMP_NTZ(order_purchase_timestamp)      AS ordered_at,
        TRY_TO_TIMESTAMP_NTZ(order_approved_at)             AS approved_at,
        TRY_TO_TIMESTAMP_NTZ(order_delivered_carrier_date)  AS shipped_at,
        TRY_TO_TIMESTAMP_NTZ(order_delivered_customer_date) AS delivered_at,
        TRY_TO_TIMESTAMP_NTZ(order_estimated_delivery_date) AS estimated_delivery_at
    FROM source
    WHERE order_purchase_timestamp IS NOT NULL
)

SELECT * FROM renamed