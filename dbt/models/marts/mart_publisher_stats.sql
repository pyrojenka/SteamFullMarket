{{
    config(
        materialized='table',
        cluster_by=["publisher_name"]
    )
}}

WITH apps AS (
    SELECT *
    FROM {{ ref('stg_applications') }}
),

app_publishers AS (
    SELECT *
    FROM {{ ref('stg_application_publishers') }}
),

publishers AS (
    SELECT *
    FROM {{ ref('stg_publishers') }}
)

SELECT
    p.publisher_name,
    COUNT(DISTINCT a.appid) AS total_games,
    COUNTIF(a.is_free = TRUE) AS free_games,
    COUNTIF(a.is_free = FALSE) AS paid_games,
    ROUND(COUNTIF(a.is_free = TRUE) * 100.0 / COUNT(*), 1) AS free_games_pct,
    ROUND(AVG(CASE
        WHEN a.mat_final_price > 0
        AND a.mat_final_price <= 20000
        AND a.mat_currency = 'USD'
        THEN a.mat_final_price / 100.0
    END), 2) AS avg_price_usd,
    ROUND(AVG(CASE
        WHEN a.mat_discount_percent IS NOT NULL
        THEN a.mat_discount_percent
        END), 1) AS avg_discount_pct
FROM app_publishers ap
JOIN apps a ON ap.appid = a.appid
JOIN publishers p ON ap.publisher_id = p.publisher_id
GROUP BY p.publisher_name
HAVING COUNT(DISTINCT a.appid) >= 10