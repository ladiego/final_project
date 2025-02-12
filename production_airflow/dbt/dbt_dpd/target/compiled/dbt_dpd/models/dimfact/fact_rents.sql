

WITH dim_books AS (

    SELECT *
    FROM `purwadika`.`diego_dwh_library_dimfact`.`dim_books`
),

dim_users AS (

    SELECT * 
    FROM `purwadika`.`diego_dwh_library_dimfact`.`dim_users`
),

rents_staging AS (

    SELECT *
    FROM `purwadika`.`diego_dwh_library_preparation`.`prep_rents`
)

SELECT
rents_staging.rent_id,
rents_staging.rent_date AS rent_date,
rents_staging.return_date AS return_date,
dim_users.user_id,
dim_users.name AS user_name,
dim_users.email AS email,
dim_books.book_id,
dim_books.title,
dim_books.author,
dim_books.release_year,
rents_staging.created_at

FROM rents_staging

LEFT JOIN dim_users
on rents_staging.user_id = dim_users.user_id

LEFT JOIN dim_books
on rents_staging.book_id = dim_books.book_id

