
  
    

    create or replace table `purwadika`.`diego_dwh_library_dimfact`.`dim_books`
      
    partition by timestamp_trunc(created_at, day)
    

    OPTIONS()
    as (
      

WITH books AS (
    SELECT *
    FROM `purwadika`.`diego_dwh_library_preparation`.`prep_books` 
)

SELECT * FROM books

    );
  