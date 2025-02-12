

WITH source AS (
    SELECT *
    FROM `purwadika`.`diego_library_final_project`.`books`
),

cleaned AS (
    SELECT
        book_id,
        title,
        author,
        release_year,
        created_at
    FROM source
)

SELECT * FROM cleaned
