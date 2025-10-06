-- Test for unreasonably long viewing durations (> 24 hours)
-- Catches potential data quality issues with session timestamps

{{ config(severity='warn') }}

WITH content_checks AS (
    SELECT 
        'content_detail' AS model_name,
        PARTITION_DATE,
        TV_ID,
        TOTAL_SECONDS AS duration_seconds,
        TOTAL_SECONDS / 3600.0 AS duration_hours
    FROM {{ ref('vizio_daily_fact_content_detail') }}
    WHERE TOTAL_SECONDS > 86400  -- More than 24 hours
    
    UNION ALL
    
    SELECT 
        'content_summary' AS model_name,
        PARTITION_DATE,
        TV_ID,
        TOTAL_VIEWING_SECONDS AS duration_seconds,
        TOTAL_VIEWING_SECONDS / 3600.0 AS duration_hours
    FROM {{ ref('vizio_daily_fact_content_summary') }}
    WHERE TOTAL_VIEWING_SECONDS > 86400  -- More than 24 hours
    
    UNION ALL
    
    SELECT 
        'standard_summary' AS model_name,
        PARTITION_DATE,
        TV_ID,
        TOTAL_ACTIVITY_SECONDS AS duration_seconds,
        TOTAL_ACTIVITY_SECONDS / 3600.0 AS duration_hours
    FROM {{ ref('vizio_daily_fact_standard_summary') }}
    WHERE TOTAL_ACTIVITY_SECONDS > 86400  -- More than 24 hours
)
SELECT 
    model_name,
    PARTITION_DATE,
    TV_ID,
    duration_seconds,
    ROUND(duration_hours, 2) AS duration_hours
FROM content_checks
ORDER BY duration_hours DESC
LIMIT 100

