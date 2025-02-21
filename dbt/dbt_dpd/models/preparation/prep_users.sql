{{
    config(
        materialized='incremental',
        unique_key='user_id',
            partition_by={
                "field": "created_at",
                "data_type": "timestamp"
            }
    )
}}

WITH source AS (
    SELECT *
    FROM {{ source('diego_library', 'users_data') }}
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
{% if check_if_incremental() %}
    WHERE created_at > (
        SELECT MAX(created_at)
        FROM {{ this }}
    )
{% endif %}