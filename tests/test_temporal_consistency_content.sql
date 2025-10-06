-- Test temporal consistency: session end times should be after start times
-- Ensures data quality in content detail table

{{ config(severity='error') }}

SELECT 
    PARTITION_DATE,
    TV_ID,
    SESSION_START_TIME_UTC,
    SESSION_END_TIME_UTC,
    TOTAL_SECONDS
FROM {{ ref('vizio_daily_fact_content_detail') }}
WHERE SESSION_END_TIME_UTC <= SESSION_START_TIME_UTC
   OR TOTAL_SECONDS < 0

