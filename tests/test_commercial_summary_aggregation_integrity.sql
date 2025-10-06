-- Test to ensure commercial_summary aggregations match detail table
-- Validates that summary table correctly aggregates the detail table

{{ config(severity='error') }}

WITH 
detail_agg AS (
    SELECT
        PARTITION_DATE,
        TV_ID,
        SUM(AD_LENGTH) AS detail_total_seconds,
        COUNT(*) AS detail_ad_views
    FROM {{ ref('vizio_daily_fact_commerical_detail') }}
    GROUP BY PARTITION_DATE, TV_ID
),
summary AS (
    SELECT
        PARTITION_DATE,
        TV_ID,
        TOTAL_AD_SECONDS,
        TOTAL_AD_VIEWS
    FROM {{ ref('vizio_daily_fact_commercial_summary') }}
)
SELECT 
    s.PARTITION_DATE,
    s.TV_ID,
    s.TOTAL_AD_SECONDS AS summary_seconds,
    d.detail_total_seconds,
    s.TOTAL_AD_VIEWS AS summary_views,
    d.detail_ad_views,
    ABS(s.TOTAL_AD_SECONDS - d.detail_total_seconds) AS seconds_diff,
    ABS(s.TOTAL_AD_VIEWS - d.detail_ad_views) AS views_diff
FROM summary s
INNER JOIN detail_agg d
    ON s.PARTITION_DATE = d.PARTITION_DATE
    AND s.TV_ID = d.TV_ID
WHERE ABS(s.TOTAL_AD_SECONDS - d.detail_total_seconds) > 1  -- Allow 1 second tolerance
   OR ABS(s.TOTAL_AD_VIEWS - d.detail_ad_views) > 0  -- View count must match exactly

