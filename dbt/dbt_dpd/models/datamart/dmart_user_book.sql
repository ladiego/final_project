{{
    config(
        materialized='incremental',
        unique_key=['user_id', 'created_at_date'],
        partition_by={
            "field": "created_at_date",
            "data_type": "date"
        }
    )
}}

WITH fact_rents AS (
    SELECT *
    FROM {{ ref('fact_rents') }}
),

dim_users AS (
    SELECT *
    FROM {{ ref('dim_users') }}
),

dim_books AS (
    SELECT *
    FROM {{ ref('dim_books') }}
),

final AS (
    SELECT DISTINCT
        fact_rents.user_id,
        fact_rents.user_name,
        dim_users.email,
        dim_users.gender,
        COUNT(fact_rents.book_id) AS sum_of_book, 
        COALESCE(CEIL(AVG(GREATEST(DATE_DIFF(fact_rents.created_at, fact_rents.return_date, DAY),0))), 0) AS avg_rent_duration, 
        DATE(fact_rents.created_at) AS created_at_date
    FROM fact_rents 
    LEFT JOIN dim_users  ON fact_rents.user_id = dim_users.user_id
    LEFT JOIN dim_books  ON fact_rents.book_id = dim_books.book_id
    GROUP BY fact_rents.user_id, fact_rents.user_name, dim_users.email, dim_users.gender, DATE(fact_rents.created_at)
    ORDER BY sum_of_book DESC
)

SELECT * FROM final