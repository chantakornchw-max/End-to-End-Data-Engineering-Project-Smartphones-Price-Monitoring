{{ config(materialized='view') }}

WITH raw_data AS (
    SELECT 
        * 
    FROM {{ source('smartphone_raw_source', 'raw_smartphone_pricing') }} 
) , 

extracted_data AS (
    SELECT
        SAFE_CAST(JSON_VALUE(data, '$.title') AS STRING) AS product_name,
        SAFE_CAST(JSON_VALUE(data, '$.product_id') AS STRING) AS product_id,
        SAFE_CAST(JSON_VALUE(data, '$.product_link') AS STRING) AS product_link,
        SAFE_CAST(JSON_VALUE(data, '$.immersive_product_page_token') AS STRING),
        SAFE_CAST(JSON_VALUE(data, '$.serpapi_immersive_product_api') AS STRING),
        SAFE_CAST(JSON_VALUE(data, '$.source') AS STRING) AS source,
        SAFE_CAST(JSON_VALUE(data, '$.source_icon') AS STRING),
        SAFE_CAST(JSON_VALUE(data, '$.price') AS STRING) AS tag_price,
        SAFE_CAST(JSON_VALUE(data, '$.extracted_price') AS FLOAT64) AS price,
        SAFE_CAST(JSON_VALUE(data, '$.rating') AS FLOAT64) AS rating,
        SAFE_CAST(JSON_VALUE(data, '$.reviews') AS FLOAT64) AS reviews,
        SAFE_CAST(JSON_VALUE(data, '$.serpapi_thumbnail') AS STRING) AS thumbnail_image,
        SAFE_CAST(JSON_VALUE(data, '$.master_product_name') AS STRING) AS tag_name,
        SAFE_CAST(JSON_VALUE(data, '$.extracted_date') AS DATE ) AS extracted_date,
        ingested_at,
        CURRENT_TIMESTAMP() AS processed_at
    FROM raw_data
),

cleaned_and_transformed_data AS (
    SELECT
        product_name,
        CASE 
            WHEN REGEXP_CONTAINS(LOWER(product_name), r'iphone.*17.*pro.*max.*256.*gb') 
                THEN 'Apple iPhone 17 Pro Max 256GB'

            WHEN REGEXP_CONTAINS(LOWER(product_name), r'samsung.*galaxy.*s26.*ultra.*256.*gb') 
                THEN 'Samsung Galaxy S26 Ultra 5G (12+256GB)'

            WHEN REGEXP_CONTAINS(LOWER(product_name), r'xiaomi.*17.*ultra.*512.*gb') 
                THEN 'Xiaomi 17 Ultra 5G (16+512GB)'

            WHEN REGEXP_CONTAINS(LOWER(product_name), r'oppo.*find.*x9.*pro.*512.*gb') 
                THEN 'OPPO Find X9 Pro 5G (16+512GB)'

            WHEN REGEXP_CONTAINS(LOWER(product_name), r'vivo.*x300.*pro.*512.*gb') 
                THEN 'vivo X300 Pro 5G (16+512GB)'

            ELSE NULL 
        END AS products,

        source, 
        CASE 
            WHEN REGEXP_CONTAINS(source, r'(?i)Banana') THEN 'BaNANA'
            WHEN REGEXP_CONTAINS(source, r'(?i)iStudio|Copperwired|SPVi|UFICON') THEN 'iStudio'
            WHEN REGEXP_CONTAINS(source, r'(?i)Studio.*7') THEN 'Studio 7'
            WHEN REGEXP_CONTAINS(source, r'(?i)JIB') THEN 'JIB'
            WHEN REGEXP_CONTAINS(source, r'(?i)Advice') THEN 'Advice'
            WHEN REGEXP_CONTAINS(source, r'(?i)Power.*Buy') THEN 'Power Buy'
            WHEN REGEXP_CONTAINS(source, r'(?i)Siam.*TV') THEN 'SiamTV'
            WHEN REGEXP_CONTAINS(source, r'(?i)Houk.*Bank') THEN 'Houk & Bank'
            WHEN REGEXP_CONTAINS(source, r'(?i)shopbkk') THEN 'shopbkk'
            WHEN REGEXP_CONTAINS(source, r'(?i)OFM') THEN 'OfficeMate'
            WHEN REGEXP_CONTAINS(source, r'(?i)ALL.*Online') THEN 'ALL Online'
            WHEN REGEXP_CONTAINS(source, r'(?i)Central') THEN 'Central Online'
            WHEN REGEXP_CONTAINS(source, r'(?i)AIS') THEN 'AIS Store'
            WHEN REGEXP_CONTAINS(source, r'(?i)True|Dtac') THEN 'True Store'
            WHEN REGEXP_CONTAINS(source, r'(?i)Shopee') THEN 'Shopee'
            WHEN REGEXP_CONTAINS(source, r'(?i)Lazada') THEN 'Lazada'
            WHEN REGEXP_CONTAINS(source, r'(?i)TikTok') THEN 'TikTok Shop'
            ELSE 'Other Sellers' 
        END AS sellers,

        NOT REGEXP_CONTAINS(source, r'(?i)ebay|etoren|hgspot|glob') AS local_th,
        price,
        rating,
        reviews,
        tag_name,
        extracted_date,
        DATETIME(ingested_at, "Asia/Bangkok") AS ingested_at_local,
        DATETIME(processed_at, "Asia/Bangkok") AS processed_at_local
    FROM extracted_data  
)

SELECT 
    product_name,
    products,
    source,
    sellers,
    price,
    rating,
    reviews,
    tag_name,
    extracted_date,
    ingested_at_local,
    processed_at_local,
    CASE 
        WHEN EXTRACT(HOUR FROM ingested_at_local) BETWEEN 6 AND 18 THEN 'Morning'
        ELSE 'Night'
    END AS run_phase
FROM cleaned_and_transformed_data
WHERE products IS NOT NULL
    AND price BETWEEN 30000 AND 60000 
    AND local_th = TRUE
QUALIFY ROW_NUMBER() OVER(
    PARTITION BY 
        products, 
        sellers, 
        extracted_date,
        run_phase 
    ORDER BY ingested_at_local DESC 
) = 1
ORDER BY extracted_date ASC, run_phase ASC



    



