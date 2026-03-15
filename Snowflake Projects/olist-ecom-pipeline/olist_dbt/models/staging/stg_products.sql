WITH source AS (
    SELECT * FROM {{ source('raw', 'products') }}
),
translations AS (
    SELECT * FROM {{ source('raw', 'product_category_translations') }}
),

joined AS (
    SELECT
        p.product_id,
        -- Use English name where available, fall back to Portuguese
        -- use the English name if it exists, otherwise fall back to the Portuguese name. COALESCE returns the first non-NULL value from left to right
        COALESCE(t.product_category_name_english, p.product_category_name) AS category,
        TRY_TO_NUMBER(p.product_weight_g, 10, 0)  AS weight_grams,
        TRY_TO_NUMBER(p.product_photos_qty, 5, 0) AS photo_count
    FROM source        p
    LEFT JOIN translations t
        ON p.product_category_name = t.product_category_name
)

SELECT * FROM joined
