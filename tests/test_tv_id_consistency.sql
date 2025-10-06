-- Test that TV_IDs exist across all fact tables (referential integrity check)
-- Ensures no orphaned TV_IDs in summary tables

{{ config(severity='warn') }}

WITH all_tv_ids AS (
    SELECT DISTINCT TV_ID, 'content_summary' AS source FROM {{ ref('vizio_daily_fact_content_summary') }}
    UNION ALL
    SELECT DISTINCT TV_ID, 'content_detail' AS source FROM {{ ref('vizio_daily_fact_content_detail') }}
    UNION ALL
    SELECT DISTINCT TV_ID, 'commercial_summary' AS source FROM {{ ref('vizio_daily_fact_commercial_summary') }}
    UNION ALL
    SELECT DISTINCT TV_ID, 'commercial_detail' AS source FROM {{ ref('vizio_daily_fact_commerical_detail') }}
    UNION ALL
    SELECT DISTINCT TV_ID, 'standard_summary' AS source FROM {{ ref('vizio_daily_fact_standard_summary') }}
    UNION ALL
    SELECT DISTINCT TV_ID, 'standard_detail' AS source FROM {{ ref('vizio_daily_fact_standard_detail') }}
),
tv_id_counts AS (
    SELECT 
        TV_ID,
        COUNT(DISTINCT source) AS table_count,
        string_agg(DISTINCT source, ', ') AS tables
    FROM all_tv_ids
    GROUP BY TV_ID
)
-- Flag TV_IDs that only appear in one table (potential data quality issue)
SELECT 
    TV_ID,
    table_count,
    tables
FROM tv_id_counts
WHERE table_count = 1
LIMIT 100  -- Limit to prevent overwhelming results

