
  
    

    create or replace table `purwadika`.`diego_dwh_library_dimfact`.`dim_users`
      
    partition by timestamp_trunc(created_at, day)
    

    OPTIONS()
    as (
      

WITH source AS (
    SELECT *
    FROM `purwadika`.`diego_dwh_library_preparation`.`prep_users`
)

SELECT * FROM source

    );
  