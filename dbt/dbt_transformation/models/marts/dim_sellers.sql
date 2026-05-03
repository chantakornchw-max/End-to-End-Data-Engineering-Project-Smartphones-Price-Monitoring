{{ config(materialized='table') }}

SELECT 
    DISTINCT sellers,
    CASE 

        WHEN sellers IN ('AIS Store', 'True Store', 'iStudio', 'Studio 7') 
            THEN 'Official Store'
            
        WHEN sellers IN ('BaNANA', 'JIB', 'Advice', 'Power Buy', 'SiamTV', 
            'Central Online', 'ALL Online', 'Houk & Bank', 'OfficeMate'
            ) 
            THEN 'Modern Trade Retailers'
            
        WHEN sellers IN ('Shopee', 'Lazada', 'TikTok Shop') 
            THEN 'E-Commerce Platform'
            
        ELSE 'Other'
    END AS seller_type
FROM {{ ref('stg_smartphone_pricing') }}
WHERE sellers IS NOT NULL
