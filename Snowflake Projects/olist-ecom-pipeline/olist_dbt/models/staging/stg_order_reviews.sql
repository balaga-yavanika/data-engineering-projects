WITH source AS (
    SELECT * FROM {{ source('raw', 'order_reviews') }}
),

renamed AS (
    SELECT
        review_id,
        order_id,
        review_score::INT                           AS score,
        review_comment_message                      AS comment,
        TRY_TO_TIMESTAMP_NTZ(review_creation_date)  AS reviewed_at
    FROM source
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY reviewed_at DESC
        ) AS rn
    FROM renamed
)

SELECT
    review_id,
    order_id,
    score,
    comment,
    reviewed_at
FROM deduped
WHERE rn = 1
