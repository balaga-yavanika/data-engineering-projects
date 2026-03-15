WITH source AS (
    SELECT * FROM {{ source('raw', 'order_items') }}
),

renamed AS (
    SELECT
        order_id,
        order_item_id::INT                              AS item_sequence,
        -- casts the text "1" to the integer 1.
        product_id,
        seller_id,
        TRY_TO_TIMESTAMP_NTZ(shipping_limit_date)       AS ship_by,
        TRY_TO_NUMBER(price,        18, 2)              AS unit_price, 
        -- converts text "129.90" to a decimal number. The 18, 2 means: up to 18 digits total, 2 decimal places.
        TRY_TO_NUMBER(freight_value, 18, 2)             AS freight_value
    FROM source
)

SELECT * FROM renamed
