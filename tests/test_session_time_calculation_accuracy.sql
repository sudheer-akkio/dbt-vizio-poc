-- Test that TOTAL_SECONDS matches the calculated difference between start and end times
-- Validates data transformation accuracy

{{ config(severity='warn') }}

WITH content_time_checks AS (
    SELECT 
        'content_detail' AS model_name,
        PARTITION_DATE,
        TV_ID,
        TOTAL_SECONDS AS reported_seconds,
        DATEDIFF(SECOND, SESSION_START_TIME_UTC, SESSION_END_TIME_UTC) AS calculated_seconds,
        ABS(TOTAL_SECONDS - DATEDIFF(SECOND, SESSION_START_TIME_UTC, SESSION_END_TIME_UTC)) AS diff
    FROM {{ ref('vizio_daily_fact_content_detail') }}
    WHERE SESSION_START_TIME_UTC IS NOT NULL 
      AND SESSION_END_TIME_UTC IS NOT NULL
),
standard_time_checks AS (
    SELECT 
        'standard_detail' AS model_name,
        PARTITION_DATE,
        TV_ID,
        TOTAL_SECONDS AS reported_seconds,
        DATEDIFF(SECOND, SESSION_START_TIME_UTC, SESSION_END_TIME_UTC) AS calculated_seconds,
        ABS(TOTAL_SECONDS - DATEDIFF(SECOND, SESSION_START_TIME_UTC, SESSION_END_TIME_UTC)) AS diff
    FROM {{ ref('vizio_daily_fact_standard_detail') }}
    WHERE SESSION_START_TIME_UTC IS NOT NULL 
      AND SESSION_END_TIME_UTC IS NOT NULL
),
all_checks AS (
    SELECT * FROM content_time_checks
    UNION ALL
    SELECT * FROM standard_time_checks
)
SELECT 
    model_name,
    PARTITION_DATE,
    TV_ID,
    reported_seconds,
    calculated_seconds,
    diff AS seconds_difference
FROM all_checks
WHERE diff > 1  -- Allow 1 second tolerance for rounding
LIMIT 100

