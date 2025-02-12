{{
    config(
        materialized='incremental',
        unique_key=['user_id', 'rent_date'],
        partition_by={
            "field": "rent_date",
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
)

SELECT 
    fact_rents.user_id,
    fact_rents.user_name,
    dim_users.email,
    DATE(fact_rents.created_at) AS rent_date,
    COUNT(fact_rents.book_id) AS sum_of_book, -- Total jumlah buku yang disewa
    COALESCE(CEIL(AVG(GREATEST(DATE_DIFF(fact_rents.created_at, fact_rents.return_date, DAY),0))), 0) AS avg_rent_duration, -- Rata-rata lama peminjaman dalam hari
    COALESCE(MIN(dim_books.release_year), 0) AS oldest_book_rented, -- Tahun rilis buku tertua yang dipinjam oleh user
    fact_rents.created_at

FROM fact_rents 
LEFT JOIN dim_users  ON fact_rents.user_id = dim_users.user_id
LEFT JOIN dim_books  ON fact_rents.book_id = dim_books.book_id
GROUP BY fact_rents.user_id, fact_rents.user_name, dim_users.email, rent_date, fact_rents.created_at