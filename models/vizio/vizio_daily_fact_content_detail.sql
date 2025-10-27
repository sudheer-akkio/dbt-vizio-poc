{{ config(
    alias='VIZIO_DAILY_FACT_CONTENT_DETAIL',
    materialized='incremental',
    post_hook=[    
        "alter table {{this}} cluster by (viewed_date, akkio_id)", 
    ]
)}}

WITH 
content AS (
	SELECT *
	FROM {{ source('vizio_poc_share', 'production_r2079_content_with_null') }}
    {% if var('start_date', None) and var('end_date', None) %}
        -- Batch processing mode: use --vars '{"start_date": "2024-01-01", "end_date": "2024-01-31"}'
        WHERE date_partition BETWEEN '{{ var("start_date") }}' AND '{{ var("end_date") }}'
    {% elif is_incremental() %}
        -- Normal incremental mode: process only new data
        WHERE date_partition > (SELECT MAX(PARTITION_DATE) FROM {{ this }})
    {% endif %}
),
genre_mapping AS (SELECT * FROM {{ source('vizio_poc_share', 'mk_akkio_genre_title_mapping') }}),
timezone_mapping AS (SELECT * FROM {{ source('vizio_poc_share', 'mk_akkio_tvtimezone_mapping') }}),
enriched_content AS (
    SELECT
        c.date_partition AS PARTITION_DATE,
        c.hash AS TV_ID,
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
        c.live AS LIVE,
        c.input_category,
        c.input_device AS INPUT_DEVICE_NAME,
        c.app_service,
        replace(g.genre, ', ', ',') AS PROGRAM_GENRE,
        tz.timezone AS TIMEZONE
    FROM content c
    LEFT JOIN genre_mapping g
        ON c.episode_id = g.episode_id
    LEFT JOIN timezone_mapping tz
        ON c.hash = tz.hash
    WHERE c.show_title IS NOT NULL 
        AND c.hash IS NOT NULL
)
SELECT 
    PARTITION_DATE,
    PARTITION_DATE AS VIEWED_DATE,
    TV_ID AS AKKIO_ID,
    ZIP_CODE,
    DMA,
    TIMEZONE,
    lower(replace(NETWORK, ' ', '-')) AS NETWORK,
    lower(replace(CALLSIGN, ' ', '-')) AS CALLSIGN,
    PROGRAM_EPISODE_ID,
    lower(replace(PROGRAM_SERIES_TITLE, ' ', '-')) AS TITLE,
    lower(replace(PROGRAM_GENRE, ' ', '-')) AS GENRE,
    AIR_DATE,
    LIVE AS WATCHED_LIVE,
    SESSION_START_TIME_UTC,
    SESSION_END_TIME_UTC,
    TOTAL_SECONDS,
    upper(replace(INPUT_CATEGORY, ' ', '-')) AS INPUT_CATEGORY,
    lower(replace(INPUT_DEVICE_NAME, ' ', '_')) AS INPUT_DEVICE_NAME,
    lower(replace(APP_SERVICE, ' ', '-')) AS APP_SERVICE
FROM enriched_content
WHERE TOTAL_SECONDS > 10 -- filter for something that's been watched atleast 10s

