{{ config(
    materialized='table',
    post_hook=[    
        "alter table {{this}} cluster by (partition_date, tv_id)", 
    ]
)}}

WITH 
content AS (SELECT * FROM {{ source('vizio_poc_share', 'production_r2079_content_with_null') }}),
genre_mapping AS (SELECT * FROM {{ source('vizio_poc_share', 'mk_akkio_genre_title_mapping') }}),
timezone_mapping AS (SELECT * FROM {{ source('vizio_poc_share', 'mk_akkio_tvtimezone_mapping') }}),
raw_content AS (
    SELECT
        c.date_partition AS PARTITION_DATE,
        c.date_partition AS VIEWED_DATE,
        c.hash AS TV_ID,
        c.ip AS HASHED_IP,
        c.zipcode AS ZIP_CODE,
        c.dma,
        c.channel_affiliate AS NETWORK,
        c.channel_callsign AS CALLSIGN,
        c.episode_id AS PROGRAM_EPISODE_ID,
        c.show_title AS PROGRAM_SERIES_TITLE,
        c.air_date,
        c.ts_start AS SESSION_START_TIME_UTC,
        c.ts_end AS SESSION_END_TIME_UTC,
        DATEDIFF(SECOND, c.ts_start, c.ts_end) AS TOTAL_SECONDS,
        c.live AS SESSION_TYPE,
        c.input_category,
        c.input_device AS INPUT_DEVICE_NAME,
        c.app_service,
        g.genre AS PROGRAM_GENRE
    FROM content c
    LEFT JOIN genre_mapping g
        ON c.episode_id = g.episode_id
    WHERE c.show_title IS NOT NULL 
        AND c.hash IS NOT NULL
),
enriched_content AS (
    SELECT 
        rc.*,
        tz.timezone AS TIMEZONE
    FROM raw_content rc
    LEFT JOIN timezone_mapping tz
        ON rc.TV_ID = tz.hash
)
SELECT 
    PARTITION_DATE,
    VIEWED_DATE,
    TV_ID,
    HASHED_IP,
    ZIP_CODE,
    DMA,
    TIMEZONE,
    lower(replace(NETWORK, ' ', '-')) AS NETWORK,
    lower(replace(CALLSIGN, ' ', '-')) AS CALLSIGN,
    PROGRAM_EPISODE_ID,
    lower(replace(PROGRAM_SERIES_TITLE, ' ', '-')) AS TITLE,
    lower(replace(PROGRAM_GENRE, ' ', '-')) AS GENRE,
    AIR_DATE,
    SESSION_TYPE,
    SESSION_START_TIME_UTC,
    SESSION_END_TIME_UTC,
    TOTAL_SECONDS,
    INPUT_CATEGORY,
    lower(replace(INPUT_DEVICE_NAME, ' ', '-')) AS INPUT_DEVICE_NAME,
    lower(replace(APP_SERVICE, ' ', '-')) AS APP_SERVICE
FROM enriched_content

