WITH source AS (
    SELECT *
    FROM `steam_fullmarket.raw_genres`
)

SELECT
    id AS genre_id,
    name AS genre_description
FROM source
WHERE name IS NOT NULL