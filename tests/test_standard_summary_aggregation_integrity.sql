-- Test to ensure standard_summary aggregations match detail table
-- Validates that summary table correctly aggregates the detail table

{{ config(severity='error') }}

WITH 
detail_agg AS (
    SELECT
        PARTITION_DATE,
        TV_ID,
        SUM(TOTAL_SECONDS) AS detail_total_seconds,
        COUNT(*) AS detail_session_count,
        MIN(SESSION_START_TIME_UTC) AS detail_first_activity,
        MAX(SESSION_END_TIME_UTC) AS detail_last_activity
    FROM {{ ref('vizio_daily_fact_standard_detail') }}
    GROUP BY PARTITION_DATE, TV_ID
),
summary AS (
    SELECT
        PARTITION_DATE,
        TV_ID,
        TOTAL_ACTIVITY_SECONDS,
        TOTAL_SESSIONS,
        FIRST_ACTIVITY_TIME,
        LAST_ACTIVITY_TIME
    FROM {{ ref('vizio_daily_fact_standard_summary') }}
)
SELECT 
    s.PARTITION_DATE,
    s.TV_ID,
    s.TOTAL_ACTIVITY_SECONDS AS summary_seconds,
    d.detail_total_seconds,
    s.TOTAL_SESSIONS AS summary_sessions,
    d.detail_session_count,
    ABS(s.TOTAL_ACTIVITY_SECONDS - d.detail_total_seconds) AS seconds_diff,
    ABS(s.TOTAL_SESSIONS - d.detail_session_count) AS sessions_diff
FROM summary s
INNER JOIN detail_agg d
    ON s.PARTITION_DATE = d.PARTITION_DATE
    AND s.TV_ID = d.TV_ID
WHERE ABS(s.TOTAL_ACTIVITY_SECONDS - d.detail_total_seconds) > 1  -- Allow 1 second tolerance
   OR ABS(s.TOTAL_SESSIONS - d.detail_session_count) > 0  -- Session count must match exactly
   OR s.FIRST_ACTIVITY_TIME != d.detail_first_activity
   OR s.LAST_ACTIVITY_TIME != d.detail_last_activity

