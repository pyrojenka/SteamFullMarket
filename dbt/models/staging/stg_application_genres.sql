WITH source AS (
    SELECT *
    FROM `steam_fullmarket.raw_application_genres`
)

SELECT
    appid,
    genre_id
FROM source
WHERE appid IS NOT NULL
  AND genre_id IS NOT NULL