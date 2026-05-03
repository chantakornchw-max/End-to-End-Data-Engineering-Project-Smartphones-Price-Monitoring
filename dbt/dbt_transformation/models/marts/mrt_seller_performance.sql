{{ config(
    materialized='table',
    partition_by={
      "field": "extracted_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by=['sellers']
) }}

WITH base_data AS (
    SELECT
        *,
        AVG(price) OVER(PARTITION BY products, extracted_date, run_phase) AS market_avg_price,
        MIN(price) OVER(PARTITION BY products, extracted_date, run_phase) AS market_min_price
    FROM {{ ref('stg_smartphone_pricing') }}
),

seller_metrics AS (
    SELECT
        extracted_date,
        run_phase,       
        sellers,
        products,
        COUNT(DISTINCT products) AS total_product_models,
        COUNT(*) AS total_listings,
        ROUND(AVG(price), 2) AS avg_seller_price,
        SUM(CASE WHEN price = market_min_price THEN 1 ELSE 0 END) AS times_being_cheapest,
        ROUND(AVG(rating), 2) AS avg_ratings,
        ROUND(AVG(reviews), 2) AS avg_reviews,
        ROUND(AVG((price - market_avg_price) / market_avg_price) * 100, 2) AS avg_price_diff_pct
    FROM base_data
    GROUP BY 1, 2, 3, 4
)

SELECT
    extracted_date,
    run_phase,
    products,       
    sellers,    
    total_product_models,
    total_listings,
    avg_seller_price,
    times_being_cheapest,
    avg_ratings,
    avg_reviews,
    avg_price_diff_pct,
    ROUND(((100 - avg_price_diff_pct) * COALESCE(avg_ratings, 4.0)) / 10, 2) AS value_score
FROM seller_metrics
