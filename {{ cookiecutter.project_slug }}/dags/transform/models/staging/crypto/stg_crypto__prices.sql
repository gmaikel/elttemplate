WITH source AS(
    SELECT
        *
    FROM {{ source('crypto', 'prices') }}
),

staged AS(
    SELECT
        *
    FROM source
)

SELECT
    *
FROM staged