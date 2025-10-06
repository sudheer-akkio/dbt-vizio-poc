{{ config(
    materialized='table',
    unique_key = ['PARTITION_DATE', 'TV_ID'],
    post_hook=[
        "alter table {{this}} cluster by (partition_date, tv_id)"         
    ]
)}}

WITH 
detail AS (SELECT * FROM {{ ref('vizio_daily_fact_standard_detail') }} )
SELECT
    PARTITION_DATE,
    PARTITION_DATE AS ACTIVITY_DATE,
    TV_ID,
    MAX(TIMEZONE) AS TIMEZONE,
    collect_set(ZIP_CODE) AS ZIP_CODE_ARRAY,
    collect_set(DMA) AS DMA_ARRAY,
    collect_set(CITY) AS CITY_ARRAY,
    collect_set(STATE_CODE) AS STATE_CODE_ARRAY,
    string_agg(DISTINCT ZIP_CODE, '|') AS ZIP_CODE_STR_LIST,
    string_agg(DISTINCT DMA, '|') AS DMA_STR_LIST,
    string_agg(DISTINCT CITY, '|') AS CITY_STR_LIST,
    string_agg(DISTINCT STATE_CODE, '|') AS STATE_CODE_STR_LIST,
    MIN(SESSION_START_TIME_UTC) AS FIRST_ACTIVITY_TIME,
    MAX(SESSION_END_TIME_UTC) AS LAST_ACTIVITY_TIME,
    SUM(TOTAL_SECONDS) AS TOTAL_ACTIVITY_SECONDS,
    COUNT(*) AS TOTAL_SESSIONS
FROM
    detail
GROUP BY
    PARTITION_DATE, TV_ID

