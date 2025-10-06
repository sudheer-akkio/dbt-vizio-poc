-- Test that PARTITION_DATE matches the date component of timestamps
-- Ensures partition alignment for performance and correctness

{{ config(severity='warn') }}

WITH all_models AS (
    SELECT 
        'content_detail' AS model_name,
        PARTITION_DATE,
        DATE(SESSION_START_TIME_UTC) AS actual_date
    FROM {{ ref('vizio_daily_fact_content_detail') }}
    
    UNION ALL
    
    SELECT 
        'commercial_detail' AS model_name,
        PARTITION_DATE,
        DATE(AD_MATCH_START_TIME_UTC) AS actual_date
    FROM {{ ref('vizio_daily_fact_commerical_detail') }}
    
    UNION ALL
    
    SELECT 
        'standard_detail' AS model_name,
        PARTITION_DATE,
        DATE(SESSION_START_TIME_UTC) AS actual_date
    FROM {{ ref('vizio_daily_fact_standard_detail') }}
    
    UNION ALL
    
    SELECT 
        'campaign_attribution' AS model_name,
        PARTITION_DATE,
        DATE(IMPRESSION_TIMESTAMP) AS actual_date
    FROM {{ ref('vizio_campaign_attribution') }}
)
SELECT 
    model_name,
    PARTITION_DATE,
    actual_date,
    COUNT(*) AS mismatched_rows
FROM all_models
WHERE PARTITION_DATE != actual_date
GROUP BY model_name, PARTITION_DATE, actual_date

