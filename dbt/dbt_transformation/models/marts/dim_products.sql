{{ config(materialized='table') }}

SELECT DISTINCT products
FROM {{ ref('stg_smartphone_pricing') }}
WHERE products IS NOT NULL