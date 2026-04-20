{{
    config(
        materialized='table',
        cluster_by=["genre_description"]
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
        a.is_free,
        a.mat_final_price,
        a.mat_currency,
        a.metacritic_score,
        a.release_year
    FROM app_genres ag
    JOIN apps a ON ag.appid = a.appid
    JOIN genres g ON ag.genre_id = g.genre_id
),

aggregated AS (
    SELECT
        genre_description,
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
    FROM joined
    GROUP BY genre_description
)

SELECT
    genre_description,
    total_games,
    free_games,
    paid_games,
    avg_price_usd,
    avg_metacritic_score,
    ROUND(free_games * 100.0 / total_games, 1) AS free_games_pct
FROM aggregated