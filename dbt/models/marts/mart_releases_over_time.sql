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
        cluster_by=["release_year"]
    )
}}

WITH apps AS (
    SELECT *
    FROM {{ ref('stg_applications') }}
),

yearly_stats AS (
    SELECT
        release_year,
        COUNT(*) AS total_games,
        COUNTIF(is_free = TRUE) AS free_games,
        COUNTIF(is_free = FALSE) AS paid_games,
        ROUND(AVG(CASE 
            WHEN mat_final_price > 0 
            AND mat_final_price <= 20000 
            AND mat_currency = 'USD'
            THEN mat_final_price / 100.0 
        END), 2) AS avg_price_usd,
        ROUND(AVG(metacritic_score), 1) AS avg_metacritic_score
    FROM apps
    GROUP BY release_year
)

SELECT
    release_year,
    total_games,
    free_games,
    paid_games,
    avg_price_usd,
    avg_metacritic_score,
    ROUND(free_games * 100.0 / total_games, 1) AS free_games_pct
FROM yearly_stats