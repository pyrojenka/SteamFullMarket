{{
    config(
        materialized='table',
        partition_by={
            "field": "release_year",
            "data_type": "int64",
            "range": {
                "start": 2003,
                "end": 2026,
                "interval": 1
            }
        },
        cluster_by=["genre_description", "release_year"]
    )
}}

WITH apps AS (
    SELECT *
    FROM {{ ref('stg_applications') }}
),

app_genres AS (
    SELECT *
    FROM {{ ref('stg_application_genres') }}
),

genres AS (
    SELECT *
    FROM {{ ref('stg_genres') }}
),

joined AS (
    SELECT
        g.genre_description,
        a.release_year,
        a.is_free,
        a.mat_final_price,
        a.mat_currency
    FROM app_genres ag
    JOIN apps a ON ag.appid = a.appid
    JOIN genres g ON ag.genre_id = g.genre_id
)

SELECT
    genre_description,
    release_year,
    COUNT(*) AS total_games,
    COUNTIF(is_free = TRUE) AS free_games,
    ROUND(COUNTIF(is_free = TRUE) * 100.0 / COUNT(*), 1) AS free_games_pct,
    ROUND(AVG(CASE 
        WHEN mat_final_price > 0 
        AND mat_final_price <= 20000
        AND mat_currency = 'USD'
        THEN mat_final_price / 100.0 
    END), 2) AS avg_price_usd
FROM joined
GROUP BY genre_description, release_year