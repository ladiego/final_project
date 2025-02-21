{{
    config(
        materialized='incremental',
        unique_key='book_id',
            partition_by={
                "field": "created_at",
                "data_type": "timestamp"
            }
    )
}}

WITH source AS (
    SELECT *
    FROM {{ source('diego_library', 'books_data') }}
),

cleaned AS (
    SELECT
        book_id,
        title,
        author,
        book_type,
        genre,
        release_year,
        stock,
        created_at
    FROM source
)

SELECT * FROM cleaned
{% if check_if_incremental() %}
    WHERE created_at > (
        SELECT MAX(created_at)
        FROM {{ this }}
    )
{% endif %}