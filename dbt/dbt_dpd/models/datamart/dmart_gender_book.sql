{{
    config(
        materialized='incremental',
        unique_key=['gender', 'book_type', 'genre', 'created_at_date'],
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

final as (
    SELECT DISTINCT
        du.gender,
        db.book_type,
        db.genre,
        COUNT(fr.book_id) AS total_rentals,
        DATE(fr.created_at) AS created_at_date
    FROM fact_rents fr
    LEFT JOIN dim_users du ON fr.user_id = du.user_id
    LEFT JOIN dim_books db ON fr.book_id = db.book_id
    GROUP BY du.gender, db.book_type, db.genre, DATE(fr.created_at)
    ORDER BY du.gender, total_rentals DESC
)

SELECT * FROM final