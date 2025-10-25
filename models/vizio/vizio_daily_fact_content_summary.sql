{{ config(
    alias='PV_VIZIO_DAILY_FACT_CONTENT_SUMMARY',
    materialized='table',
    unique_key = ['PARTITION_DATE', 'AKKIO_ID'],
    post_hook=[
        "alter table {{this}} cluster by (viewed_date, akkio_id)"         
    ]
)}}

WITH 
detail AS (SELECT * FROM {{ ref('vizio_daily_fact_content_detail') }} )
SELECT
    PARTITION_DATE,
    PARTITION_DATE AS VIEWED_DATE,
    AKKIO_ID AS AKKIO_ID,
    MAX(TIMEZONE) AS TIMEZONE,
    max(ZIP_CODE) AS ZIP_CODE,
    max(DMA) AS DMA,
    string_agg(DISTINCT NETWORK, ',') AS NETWORK_STR_LIST,
    string_agg(DISTINCT CALLSIGN, ',') AS CALLSIGN_STR_LIST,
    string_agg(DISTINCT TITLE, ',') AS TITLE_STR_LIST,
    array_join(ARRAY_DISTINCT(flatten(ARRAY_AGG(SPLIT(GENRE, ',')))), ',') AS GENRE_STR_LIST,
    string_agg(DISTINCT INPUT_CATEGORY, ',') AS INPUT_CATEGORY_STR_LIST,
    string_agg(DISTINCT INPUT_DEVICE_NAME, ',') AS INPUT_DEVICE_STR_LIST,
    string_agg(DISTINCT APP_SERVICE, ',') AS APP_SERVICE_STR_LIST,
    SUM(TOTAL_SECONDS) AS TOTAL_VIEWING_SECONDS
FROM
    detail
GROUP BY
    PARTITION_DATE, AKKIO_ID

