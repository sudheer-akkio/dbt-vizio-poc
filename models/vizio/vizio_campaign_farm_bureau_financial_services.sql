{{ config(
    alias='VIZIO_CAMPAIGN_FARM_BUREAU_FINANCIAL_SERVICES',
    materialized='incremental',
    post_hook=[    
        "alter table {{this}} cluster by (impression_date, akkio_id)", 
    ]
)}}

WITH 
attr_data AS (
    SELECT *
	FROM {{ source('vizio_poc_share', 'farm_bureau_financial_services_attr_data_akkio_poc') }}
	WHERE hashed_tvid IS NOT NULL
	{% if var('start_date', None) and var('end_date', None) %}
        -- Batch processing mode: use --vars '{"start_date": "2024-01-01", "end_date": "2024-01-31"}'
        AND DATE(imp_ts) BETWEEN '{{ var("start_date") }}' AND '{{ var("end_date") }}'
    {% elif is_incremental() %}
        -- Normal incremental mode: process only new data
        AND DATE(imp_ts) > (SELECT MAX(PARTITION_DATE) FROM {{ this }})
    {% endif %}
),
standard AS (SELECT * FROM {{ ref('v_akkio_attributes_latest') }}),
pop_data AS (SELECT * FROM {{ source('vizio_poc_share', 'farm_bureau_financial_services_pop_data_akkio_poc') }}),
timezone_mapping AS (SELECT * FROM {{ source('vizio_poc_share', 'mk_akkio_tvtimezone_mapping') }}),
attribution_detail AS (
    SELECT
        DATE(a.imp_ts) AS PARTITION_DATE,
        DATE(a.imp_ts) AS IMPRESSION_DATE,
        a.hashed_tvid AS TV_ID,
        a.imp_ts AS IMPRESSION_TIMESTAMP,
        a.zipcode AS ZIP_CODE,
        a.market AS MARKET,
		st.DMA_NAME AS DMA,
		st.CITY,
		st.STATE AS STATE_CODE,
        a.show_title AS SHOW_TITLE,
        a.station_call_sign AS STATION_CALL_SIGN,
        a.channel_affiliate AS CHANNEL_AFFILIATE,
        a.local_or_national AS LOCAL_OR_NATIONAL,
        a.session_type AS SESSION_TYPE,
        a.session_source AS SESSION_SOURCE,
        p.inscape_tv_population AS MARKET_TV_POPULATION,
        p.ue_population AS MARKET_UE_POPULATION,
        tz.timezone AS TIMEZONE
    FROM attr_data a
    LEFT JOIN pop_data p
        ON a.market = p.market
    LEFT JOIN timezone_mapping tz
        ON a.hashed_tvid = tz.hash
	LEFT JOIN standard st
		ON st.akkio_id = a.hashed_tvid
)
SELECT 
    PARTITION_DATE,
    IMPRESSION_DATE,
    TV_ID AS AKKIO_ID,
    IMPRESSION_TIMESTAMP,
    TIMEZONE,
    ZIP_CODE,
    MARKET,
	DMA,
	CITY,
	STATE_CODE,
    MARKET_TV_POPULATION,
    MARKET_UE_POPULATION,
    lower(replace(STATION_CALL_SIGN, ' ', '-')) AS STATION_CALL_SIGN,
    lower(replace(CHANNEL_AFFILIATE, ' ', '-')) AS CHANNEL_AFFILIATE,
    LOCAL_OR_NATIONAL,
    SESSION_TYPE,
    SESSION_SOURCE,
	lower(regexp_replace(SHOW_TITLE, '[\\s\'’,:.-]+', '-')) AS SHOW_TITLE
FROM attribution_detail

