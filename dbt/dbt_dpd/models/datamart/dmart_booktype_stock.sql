{{
    config(
        materialized='incremental',
        unique_key=['book_type'],
        partition_by=None
    )
}}

WITH fact_rents AS (
    SELECT *
    FROM {{ ref('fact_rents') }}
),
dim_books AS (
    SELECT *
    FROM {{ ref('dim_books') }}
),
book_rentals AS (
    SELECT 
        db.book_type,
        db.genre,
        COUNT(fr.book_id) AS total_rentals,
        DATE(fr.created_at) AS created_at_date
    FROM fact_rents fr
    LEFT JOIN dim_books db ON fr.book_id = db.book_id
    GROUP BY db.book_type, db.genre, DATE(fr.created_at)
),
stock_per_type AS (
    SELECT 
        book_type,
        SUM(stock) AS stock_book_per_type
    FROM diego_finpro_dimfact.dim_books
    GROUP BY book_type
)

SELECT 
    br.book_type,
    COALESCE(SUM(CASE WHEN genre = 'Non-Fiction' THEN total_rentals END), 0) AS `Non-Fiction`,
    COALESCE(SUM(CASE WHEN genre = 'Fiction' THEN total_rentals END), 0) AS Fiction,
    COALESCE(SUM(CASE WHEN genre = 'Mystery' THEN total_rentals END), 0) AS Mystery,
    COALESCE(SUM(CASE WHEN genre = 'Thriller' THEN total_rentals END), 0) AS Thriller,
    COALESCE(SUM(CASE WHEN genre = 'Romance' THEN total_rentals END), 0) AS Romance,
    COALESCE(SUM(CASE WHEN genre = 'Technology' THEN total_rentals END), 0) AS Technology,
    COALESCE(SUM(total_rentals), 0) AS total_rent_per_type,
    sp.stock_book_per_type,
    COALESCE(sp.stock_book_per_type, 0) - COALESCE(SUM(br.total_rentals), 0) AS sisa_stock
FROM book_rentals br
LEFT JOIN stock_per_type sp ON br.book_type = sp.book_type
GROUP BY br.book_type, sp.stock_book_per_type
ORDER BY br.book_type