{{ config(
    materialized='incremental',
    unique_key=['PARTITION_DATE', 'TV_ID'],
    incremental_strategy='merge',
    partition_by="PARTITION_DATE",
)}}

WITH 
-- Filter source data early to reduce dataset size
commercial_filtered AS (
    SELECT 
        date_partition,
        hash,
        ip,
        zipcode,
        dma,
        value,
        ts_start,
        ts_end,
        duration,
        brand_name,
        title,
        prev_episode_id,
        prev_title,
        prev_ts_start,
        prev_ts_end,
        prev_channel_callsign,
        prev_network_affiliate,
        next_episode_id,
        next_title,
        next_ts_start,
        next_ts_end,
        next_channel_callsign,
        next_network_affiliate,
        live,
        input_category,
        input_device,
        app_service
    FROM {{ source('vizio_poc_share', 'production_r2080_commercialfeedmodular') }}
    WHERE hash IS NOT NULL
    {% if var('start_date', None) and var('end_date', None) %}
        -- Batch processing mode: use --vars '{"start_date": "2024-01-01", "end_date": "2024-01-31"}'
        AND date_partition BETWEEN '{{ var("start_date") }}' AND '{{ var("end_date") }}'
    {% elif is_incremental() %}
        -- Normal incremental mode: process only new data
        AND date_partition > (SELECT MAX(PARTITION_DATE) FROM {{ this }})
    {% endif %}
),
-- Pre-select only needed columns from lookup tables
commercial_category AS (
    SELECT 
        value,
        commercial_category
    FROM {{ source('vizio_poc_share', 'mk_commercialcategory_mapping') }}
),
timezone_mapping AS (
    SELECT 
        hash,
        timezone
    FROM {{ source('vizio_poc_share', 'mk_akkio_tvtimezone_mapping') }}
),
-- Perform joins and transformations in single pass
enriched_commercial AS (
    SELECT
        c.date_partition AS PARTITION_DATE,
        c.date_partition AS VIEWED_DATE,
        c.hash AS TV_ID,
        c.hash AS AKKIO_ID,
        c.ip AS HASHED_IP,
        c.zipcode AS ZIP_CODE,
        c.dma AS DMA,
        tz.timezone AS TIMEZONE,
        c.value AS CREATIVE_ID,
        c.ts_start AS AD_MATCH_START_TIME_UTC,
        c.ts_end AS AD_MATCH_END_TIME_UTC,
        CAST(c.duration AS INT) AS AD_LENGTH,
        lower(replace(c.brand_name, ' ', '-')) AS BRAND_NAME,
        lower(replace(c.title, ' ', '-')) AS AD_TITLE,
        lower(replace(cc.commercial_category, ' ', '-')) AS COMMERCIAL_CATEGORY,
        c.prev_episode_id AS PREV_EPISODE_ID,
        lower(replace(c.prev_title, ' ', '-')) AS PREV_TITLE,
        c.prev_ts_start AS PREV_CONTENT_START_TIME_UTC,
        c.prev_ts_end AS PREV_CONTENT_END_TIME_UTC,
        lower(replace(c.prev_channel_callsign, ' ', '-')) AS PREV_CALLSIGN,
        lower(replace(c.prev_network_affiliate, ' ', '-')) AS PREV_NETWORK,
        c.next_episode_id AS NEXT_EPISODE_ID,
        lower(replace(c.next_title, ' ', '-')) AS NEXT_TITLE,
        c.next_ts_start AS NEXT_CONTENT_START_TIME_UTC,
        c.next_ts_end AS NEXT_CONTENT_END_TIME_UTC,
        lower(replace(c.next_channel_callsign, ' ', '-')) AS NEXT_CALLSIGN,
        lower(replace(c.next_network_affiliate, ' ', '-')) AS NEXT_NETWORK,
        c.live AS SESSION_TYPE,
        c.input_category AS INPUT_CATEGORY,
        lower(replace(c.input_device, ' ', '-')) AS INPUT_DEVICE_NAME,
        lower(replace(c.app_service, ' ', '-')) AS APP_SERVICE
    FROM commercial_filtered c
    LEFT JOIN commercial_category cc
        ON c.value = cc.value
    LEFT JOIN timezone_mapping tz
        ON c.hash = tz.hash
)
SELECT * FROM enriched_commercial

