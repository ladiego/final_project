{{
    config(
        materialized='incremental',
        unique_key='rent_id',
            partition_by={
                "field": "created_at",
                "data_type": "timestamp"
            }
    )
}}

WITH dim_books AS (

    SELECT *
    FROM {{ ref('dim_books')}}
),

dim_users AS (

    SELECT * 
    FROM {{ ref('dim_users')}}
),

rents_staging AS (

    SELECT *
    FROM {{ ref('prep_rents') }}
)

SELECT
rents_staging.rent_id,
rents_staging.rent_date AS rent_date,
rents_staging.return_date AS return_date,
dim_users.user_id,
dim_users.name AS user_name,
dim_users.email AS email,
dim_users.gender,
dim_books.book_id,
dim_books.title,
dim_books.author,
dim_books.book_type,
dim_books.genre,
dim_books.release_year,
rents_staging.created_at

FROM rents_staging

LEFT JOIN dim_users
on rents_staging.user_id = dim_users.user_id

LEFT JOIN dim_books
on rents_staging.book_id = dim_books.book_id

{% if check_if_incremental() %}
    WHERE rents_staging.created_at > (
        SELECT MAX(created_at)
        FROM {{ this }}
    )
{% endif %}