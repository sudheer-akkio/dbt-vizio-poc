-- Test temporal consistency: ad end times should be after start times
-- Ensures data quality in commercial detail table

{{ config(severity='error') }}

SELECT 
    PARTITION_DATE,
    TV_ID,
    AD_MATCH_START_TIME_UTC,
    AD_MATCH_END_TIME_UTC,
    AD_LENGTH
FROM {{ ref('vizio_daily_fact_commerical_detail') }}
WHERE AD_MATCH_END_TIME_UTC <= AD_MATCH_START_TIME_UTC
   OR AD_LENGTH < 0

