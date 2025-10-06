-- Test to ensure content_summary aggregations match detail table
-- Validates that summary table correctly aggregates the detail table

{{ config(severity='error') }}

WITH 
detail_agg AS (
    SELECT
        PARTITION_DATE,
        TV_ID,
        SUM(TOTAL_SECONDS) AS detail_total_seconds,
        COUNT(*) AS detail_row_count
    FROM {{ ref('vizio_daily_fact_content_detail') }}
    GROUP BY PARTITION_DATE, TV_ID
),
summary AS (
    SELECT
        PARTITION_DATE,
        TV_ID,
        TOTAL_VIEWING_SECONDS
    FROM {{ ref('vizio_daily_fact_content_summary') }}
)
SELECT 
    s.PARTITION_DATE,
    s.TV_ID,
    s.TOTAL_VIEWING_SECONDS AS summary_seconds,
    d.detail_total_seconds,
    ABS(s.TOTAL_VIEWING_SECONDS - d.detail_total_seconds) AS seconds_diff
FROM summary s
INNER JOIN detail_agg d
    ON s.PARTITION_DATE = d.PARTITION_DATE
    AND s.TV_ID = d.TV_ID
WHERE ABS(s.TOTAL_VIEWING_SECONDS - d.detail_total_seconds) > 1  -- Allow 1 second tolerance for rounding

