{{ config(
    materialized='table',
    partition_by={
      "field": "extracted_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by=['products']
) }}

WITH staging AS (
  SELECT 
      *,
      TRIM(products) AS clean_products,
      CASE 
          WHEN run_phase = 'Morning' THEN 1 
          ELSE 2 
      END AS phase_rank
  FROM {{ ref('stg_smartphone_pricing') }}
), 

daily_summary AS ( 
  SELECT
      extracted_date,
      run_phase,
      phase_rank,
      clean_products AS products,
      MAX(ingested_at_local) AS latest_ingested_at_local,
      COUNT(*) AS total_listings,
      MIN(price) AS min_price,
      MAX(price) AS max_price,
      ROUND(AVG(price), 2) AS avg_price,
      ROUND(MAX(price) - MIN(price), 2) AS price_range
  FROM staging
  GROUP BY 1, 2, 3, 4
),

ranked_sellers AS (
  SELECT 
      clean_products AS products,
      extracted_date,
      run_phase,
      sellers,
      ROW_NUMBER() OVER(
          PARTITION BY clean_products, extracted_date, run_phase 
          ORDER BY price ASC, rating DESC, reviews DESC, sellers ASC
          ) AS rank_low,
      ROW_NUMBER() OVER(
          PARTITION BY clean_products, extracted_date, run_phase 
          ORDER BY price DESC, rating ASC, reviews ASC, sellers ASC 
          ) AS rank_high
  FROM staging
),

joined_data AS (
  SELECT 
      d.*,
      MAX(CASE WHEN r.rank_low = 1 THEN r.sellers END) AS cheapest_seller,
      MAX(CASE WHEN r.rank_high = 1 THEN r.sellers END) AS priciest_seller
  FROM daily_summary d
  LEFT JOIN ranked_sellers r 
      ON d.products = r.products 
      AND d.extracted_date = r.extracted_date
      AND d.run_phase = r.run_phase
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
)

SELECT 
    * EXCEPT(phase_rank, latest_ingested_at_local),
    ROUND(avg_price - LAG(avg_price) OVER(
        PARTITION BY products 
        ORDER BY extracted_date ASC, phase_rank ASC
    ), 2) AS avg_price_change
FROM joined_data