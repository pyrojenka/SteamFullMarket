WITH source AS (
    SELECT *
    FROM `steam_fullmarket.raw_applications`
),

cleaned AS (
    SELECT
        appid,
        name,
        type,
        is_free,
        CAST(release_date AS DATE) AS release_date,
        EXTRACT(YEAR FROM CAST(release_date AS DATE)) AS release_year,
        metacritic_score,
        mat_initial_price,
        mat_final_price,
        mat_discount_percent,
        mat_currency,
        mat_achievement_count
    FROM source
    WHERE type = 'game'
      AND release_date IS NOT NULL
      AND CAST(release_date AS DATE) >= '2003-01-01'
      AND CAST(release_date AS DATE) <= '2025-12-31'
      AND name IS NOT NULL
)

SELECT * FROM cleaned