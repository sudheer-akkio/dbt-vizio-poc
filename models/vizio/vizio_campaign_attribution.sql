{{ config(
    materialized='table',
    post_hook=[    
        "alter table {{this}} cluster by (partition_date, tv_id)", 
    ]
)}}

WITH 
attr_data AS (SELECT * FROM {{ source('vizio_poc_share', 'nothing_bundt_cakes_attr_data_akkio_poc') }}),
pop_data AS (SELECT * FROM {{ source('vizio_poc_share', 'nothing_bundt_cakes_pop_data_akkio_poc') }}),
timezone_mapping AS (SELECT * FROM {{ source('vizio_poc_share', 'mk_akkio_tvtimezone_mapping') }}),
attribution_detail AS (
    SELECT
        DATE(a.imp_ts) AS PARTITION_DATE,
        DATE(a.imp_ts) AS IMPRESSION_DATE,
        a.hashed_tvid AS TV_ID,
        a.ip AS HASHED_IP,
        a.imp_ts AS IMPRESSION_TIMESTAMP,
        a.zipcode AS ZIP_CODE,
        a.market AS MARKET,
        a.show_title AS SHOW_TITLE,
        a.station_call_sign AS STATION_CALL_SIGN,
        a.channel_affiliate AS CHANNEL_AFFILIATE,
        a.local_or_national AS LOCAL_OR_NATIONAL,
        a.session_type AS SESSION_TYPE,
        a.session_source AS SESSION_SOURCE,
        p.inscape_tv_population AS MARKET_TV_POPULATION
    FROM attr_data a
    LEFT JOIN pop_data p
        ON a.market = p.market
    LEFT JOIN timezone_mapping tz
        ON a.hashed_tvid = tz.hash
    WHERE a.hashed_tvid IS NOT NULL
)
SELECT 
    PARTITION_DATE,
    IMPRESSION_DATE,
    TV_ID,
    HASHED_IP,
    IMPRESSION_TIMESTAMP,
    tz.timezone AS TIMEZONE,
    ZIP_CODE,
    MARKET,
    MARKET_TV_POPULATION,
    lower(replace(SHOW_TITLE, ' ', '-')) AS SHOW_TITLE,
    lower(replace(STATION_CALL_SIGN, ' ', '-')) AS STATION_CALL_SIGN,
    lower(replace(CHANNEL_AFFILIATE, ' ', '-')) AS CHANNEL_AFFILIATE,
    LOCAL_OR_NATIONAL,
    SESSION_TYPE,
    SESSION_SOURCE
FROM attribution_detail ad
LEFT JOIN timezone_mapping tz
    ON ad.TV_ID = tz.hash

