WITH source AS (
    SELECT *
    FROM `steam_fullmarket.raw_publishers`
)

SELECT
    id as publisher_id,
    name as publisher_name
FROM source
WHERE name IS NOT NULL