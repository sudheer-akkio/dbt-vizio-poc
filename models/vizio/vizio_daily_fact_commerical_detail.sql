{{ config(
    materialized='table',
    post_hook=[    
        "alter table {{this}} cluster by (partition_date, tv_id)", 
    ]
)}}

WITH 
commercial AS (SELECT * FROM {{ source('vizio_poc_share', 'production_r2080_commercialfeedmodular') }}),
commercial_category AS (SELECT * FROM {{ source('vizio_poc_share', 'mk_commercialcategory_mapping') }}),
timezone_mapping AS (SELECT * FROM {{ source('vizio_poc_share', 'mk_akkio_tvtimezone_mapping') }}),
raw_commercial AS (
    SELECT
        c.date_partition AS PARTITION_DATE,
        c.date_partition AS VIEWED_DATE,
        c.hash AS TV_ID,
        c.ip AS HASHED_IP,
        c.zipcode AS ZIP_CODE,
        c.dma,
        c.value AS CREATIVE_ID,
        c.ts_start AS AD_MATCH_START_TIME_UTC,
        c.ts_end AS AD_MATCH_END_TIME_UTC,
        CAST(c.duration AS INT) AS AD_LENGTH,
        c.brand_name AS BRAND_NAME,
        c.title AS AD_TITLE,
        c.prev_episode_id,
        c.prev_title,
        c.prev_ts_start,
        c.prev_ts_end,
        c.prev_channel_callsign,
        c.prev_network_affiliate,
        c.next_episode_id,
        c.next_title,
        c.next_ts_start,
        c.next_ts_end,
        c.next_channel_callsign,
        c.next_network_affiliate,
        c.live AS SESSION_TYPE,
        c.input_category,
        c.input_device AS INPUT_DEVICE_NAME,
        c.app_service,
        cc.commercial_category
    FROM commercial c
    LEFT JOIN commercial_category cc
        ON c.value = cc.value
    LEFT JOIN timezone_mapping tz
        ON c.hash = tz.hash
    WHERE c.hash IS NOT NULL
),
enriched_commercial AS (
    SELECT 
        rc.*,
        tz.timezone AS TIMEZONE
    FROM raw_commercial rc
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
    CREATIVE_ID,
    AD_MATCH_START_TIME_UTC,
    AD_MATCH_END_TIME_UTC,
    AD_LENGTH,
    lower(replace(BRAND_NAME, ' ', '-')) AS BRAND_NAME,
    lower(replace(AD_TITLE, ' ', '-')) AS AD_TITLE,
    lower(replace(COMMERCIAL_CATEGORY, ' ', '-')) AS COMMERCIAL_CATEGORY,
    PREV_EPISODE_ID,
    lower(replace(PREV_TITLE, ' ', '-')) AS PREV_TITLE,
    PREV_TS_START AS PREV_CONTENT_START_TIME_UTC,
    PREV_TS_END AS PREV_CONTENT_END_TIME_UTC,
    lower(replace(PREV_CHANNEL_CALLSIGN, ' ', '-')) AS PREV_CALLSIGN,
    lower(replace(PREV_NETWORK_AFFILIATE, ' ', '-')) AS PREV_NETWORK,
    NEXT_EPISODE_ID,
    lower(replace(NEXT_TITLE, ' ', '-')) AS NEXT_TITLE,
    NEXT_TS_START AS NEXT_CONTENT_START_TIME_UTC,
    NEXT_TS_END AS NEXT_CONTENT_END_TIME_UTC,
    lower(replace(NEXT_CHANNEL_CALLSIGN, ' ', '-')) AS NEXT_CALLSIGN,
    lower(replace(NEXT_NETWORK_AFFILIATE, ' ', '-')) AS NEXT_NETWORK,
    SESSION_TYPE,
    INPUT_CATEGORY,
    lower(replace(INPUT_DEVICE_NAME, ' ', '-')) AS INPUT_DEVICE_NAME,
    lower(replace(APP_SERVICE, ' ', '-')) AS APP_SERVICE
FROM enriched_commercial

