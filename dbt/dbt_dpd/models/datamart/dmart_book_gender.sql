{{
    config(
        materialized='incremental',
        unique_key=['gender', 'book_type', 'genre'],
        partition_by=None
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

genre_gender AS (
    SELECT 
        fact_rents.user_id,
        dim_users.gender,
        dim_books.book_type,
        dim_books.genre,
        COUNT(fact_rents.book_id) AS total_rentals
    FROM fact_rents  
    LEFT JOIN dim_users ON fact_rents.user_id = dim_users.user_id
    LEFT JOIN dim_books ON fact_rents.book_id = dim_books.book_id
    GROUP BY fact_rents.user_id, dim_users.gender, dim_books.book_type, dim_books.genre
),

total_per_genre AS (
    SELECT 
        book_type,
        genre,
        SUM(total_rentals) AS total_rentals_all
    FROM genre_gender
    GROUP BY book_type, genre
),

final AS (
    SELECT 
        gg.book_type,
        gg.genre,
        gg.gender,
        SUM(gg.total_rentals) AS total_rentals_gender,  -- Total peminjaman berdasarkan gender
        tp.total_rentals_all,  -- Total peminjaman untuk kombinasi book_type dan genre
        ROUND((SUM(gg.total_rentals) * 100.0 / NULLIF(tp.total_rentals_all, 0)), 2) AS percentage_per_gender  -- Persentase peminjaman
    FROM genre_gender gg
    JOIN total_per_genre tp 
    ON gg.book_type = tp.book_type AND gg.genre = tp.genre
    GROUP BY gg.book_type, gg.genre, gg.gender, tp.total_rentals_all  -- Mengelompokkan berdasarkan book_type, genre, gender
    ORDER BY gg.book_type, gg.genre, total_rentals_gender DESC  -- Mengurutkan hasil
)

SELECT * FROM final
ORDER BY book_type ASC