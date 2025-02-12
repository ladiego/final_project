
  
    

    create or replace table `purwadika`.`diego_dwh_library_staging`.`staging_rents`
      
    partition by timestamp_trunc(created_at, day)
    

    OPTIONS()
    as (
      

WITH source AS (
    SELECT *
    FROM `purwadika`.`diego_library_final_project`.`rents`
)

SELECT *
FROM source

    );
  