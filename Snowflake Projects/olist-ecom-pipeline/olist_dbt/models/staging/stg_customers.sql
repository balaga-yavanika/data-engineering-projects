WITH source AS (
    SELECT * FROM {{ source('raw', 'customers') }}
),

renamed AS (
    SELECT
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix  AS zip_prefix,
        INITCAP(customer_city)    AS city,   -- Normalise case: 'sao paulo' -> 'Sao Paulo'
        UPPER(customer_state)     AS state   -- States as 2-char codes: 'sp' -> 'SP'
    FROM source
)

SELECT * FROM RENAMED