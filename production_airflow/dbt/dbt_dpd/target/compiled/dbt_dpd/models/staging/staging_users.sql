

WITH source AS (
    SELECT *
    FROM `purwadika`.`diego_library_final_project`.`users`
),

cleaned AS (
    SELECT
        user_id,
        name,
        email,
        gender,
        created_at
    FROM source
)

SELECT * FROM cleaned
