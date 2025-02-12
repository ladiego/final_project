
  
    

    create or replace table `purwadika`.`diego_dwh_library_staging`.`staging_books`
      
    partition by timestamp_trunc(created_at, day)
    

    OPTIONS()
    as (
      

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

    );
  