WITH source AS (
    SELECT *
    FROM `steam_fullmarket.raw_application_publishers`
)

SELECT
    appid,
    publisher_id
FROM source
WHERE appid IS NOT NULL
  AND publisher_id IS NOT NULL