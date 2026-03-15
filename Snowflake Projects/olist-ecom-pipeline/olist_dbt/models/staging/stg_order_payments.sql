WITH source AS (
    SELECT * FROM {{ source('raw', 'order_payments') }}
),

renamed AS (
    SELECT
        order_id,
        payment_sequential::INT                    AS payment_sequence,
        payment_type,
        payment_installments::INT                  AS installments,
        TRY_TO_NUMBER(payment_value, 18, 2)        AS payment_amount
    FROM source
    WHERE payment_value IS NOT NULL
)

SELECT * FROM renamed
