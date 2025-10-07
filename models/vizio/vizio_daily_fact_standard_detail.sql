{{ config(
    materialized='table',
    post_hook=[    
        "alter table {{this}} cluster by (partition_date, tv_id)", 
    ]
)}}

WITH 
ipage AS (SELECT * FROM {{ source('vizio_poc_share', 'production_r2081_ipage') }}),
timezone_mapping AS (SELECT * FROM {{ source('vizio_poc_share', 'mk_akkio_tvtimezone_mapping') }})
SELECT 
    i.date_partition AS PARTITION_DATE,
    i.date_partition AS ACTIVITY_DATE,
    i.hash AS TV_ID,
    i.hash AS AKKIO_ID,
    i.ip AS HASHED_IP,
    tz.timezone AS TIMEZONE,
    i.ts_start AS SESSION_START_TIME_UTC,
    i.ts_end AS SESSION_END_TIME_UTC,
    DATEDIFF(SECOND, i.ts_start, i.ts_end) AS TOTAL_SECONDS,
    i.city AS CITY,
    i.iso_state AS STATE_CODE,
    i.dma AS DMA,
    i.zipcode AS ZIP_CODE
FROM ipage i
LEFT JOIN timezone_mapping tz
    ON i.hash = tz.hash
WHERE i.hash IS NOT NULL

