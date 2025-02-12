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

WITH books AS (
    SELECT *
    FROM {{ ref('prep_books') }} 
)

SELECT * FROM books
{% if check_if_incremental() %}
    WHERE created_at > (
        SELECT MAX(created_at)
        FROM {{ this }}
    )
{% endif %}