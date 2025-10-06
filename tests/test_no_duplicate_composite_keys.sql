-- Test for duplicate composite keys across all summary tables
-- Critical test to ensure grain is maintained correctly

{{ config(severity='error') }}

WITH content_summary_dupes AS (
    SELECT 
        'content_summary' AS model_name,
        PARTITION_DATE,
        TV_ID,
        COUNT(*) AS duplicate_count
    FROM {{ ref('vizio_daily_fact_content_summary') }}
    GROUP BY PARTITION_DATE, TV_ID
    HAVING COUNT(*) > 1
),
commercial_summary_dupes AS (
    SELECT 
        'commercial_summary' AS model_name,
        PARTITION_DATE,
        TV_ID,
        COUNT(*) AS duplicate_count
    FROM {{ ref('vizio_daily_fact_commercial_summary') }}
    GROUP BY PARTITION_DATE, TV_ID
    HAVING COUNT(*) > 1
),
standard_summary_dupes AS (
    SELECT 
        'standard_summary' AS model_name,
        PARTITION_DATE,
        TV_ID,
        COUNT(*) AS duplicate_count
    FROM {{ ref('vizio_daily_fact_standard_summary') }}
    GROUP BY PARTITION_DATE, TV_ID
    HAVING COUNT(*) > 1
),
all_dupes AS (
    SELECT * FROM content_summary_dupes
    UNION ALL
    SELECT * FROM commercial_summary_dupes
    UNION ALL
    SELECT * FROM standard_summary_dupes
)
SELECT * FROM all_dupes

