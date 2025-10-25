{{ config(
    alias='V_AGG_AKKIO_IND_MEDIA',
    materialized='table',
    post_hook=[
        "alter table {{this}} cluster by (PARTITION_DATE, AKKIO_ID)",
    ]
)}}

/*
    Vizio Individual Media Aggregation Table

    Purpose: Individual-level aggregation of media viewing behavior for analytics.
    Source: vizio_daily_fact_content_detail
    Grain: One row per PARTITION_DATE + AKKIO_ID

    Maps contain category -> session count for titles, genres, networks, devices, and app services.
    Supports LATERAL VIEW EXPLODE pattern for categorical breakdowns.
*/

WITH title_counts AS (
    SELECT
        PARTITION_DATE,
        AKKIO_ID,
        COALESCE(TITLE, 'unknown') AS TITLE_KEY,
        COUNT(*) AS SESSION_COUNT
    FROM {{ ref('vizio_daily_fact_content_detail') }}
    GROUP BY
        PARTITION_DATE,
        AKKIO_ID,
        COALESCE(TITLE, 'unknown')
),

-- Explode comma-separated genres and count sessions per genre
genre_exploded AS (
    SELECT
        PARTITION_DATE,
        AKKIO_ID,
        TRIM(genre_value) AS GENRE_KEY
    FROM {{ ref('vizio_daily_fact_content_detail') }}
    LATERAL VIEW EXPLODE(SPLIT(COALESCE(GENRE, 'unknown'), ',')) AS genre_value
),

genre_counts AS (
    SELECT
        PARTITION_DATE,
        AKKIO_ID,
        COALESCE(NULLIF(GENRE_KEY, ''), 'unknown') AS GENRE_KEY,
        COUNT(*) AS SESSION_COUNT
    FROM genre_exploded
    GROUP BY
        PARTITION_DATE,
        AKKIO_ID,
        COALESCE(NULLIF(GENRE_KEY, ''), 'unknown')
),

network_counts AS (
    SELECT
        PARTITION_DATE,
        AKKIO_ID,
        COALESCE(NETWORK, 'unknown') AS NETWORK_KEY,
        COUNT(*) AS SESSION_COUNT
    FROM {{ ref('vizio_daily_fact_content_detail') }}
    GROUP BY
        PARTITION_DATE,
        AKKIO_ID,
        COALESCE(NETWORK, 'unknown')
),

device_counts AS (
    SELECT
        PARTITION_DATE,
        AKKIO_ID,
        COALESCE(INPUT_DEVICE_NAME, INPUT_CATEGORY, 'unknown') AS DEVICE_KEY,
        COUNT(*) AS SESSION_COUNT
    FROM {{ ref('vizio_daily_fact_content_detail') }}
    GROUP BY
        PARTITION_DATE,
        AKKIO_ID,
        COALESCE(INPUT_DEVICE_NAME, INPUT_CATEGORY, 'unknown')
),

app_service_counts AS (
    SELECT
        PARTITION_DATE,
        AKKIO_ID,
        COALESCE(APP_SERVICE, 'unknown') AS APP_SERVICE_KEY,
        COUNT(*) AS SESSION_COUNT
    FROM {{ ref('vizio_daily_fact_content_detail') }}
    GROUP BY
        PARTITION_DATE,
        AKKIO_ID,
        COALESCE(APP_SERVICE, 'unknown')
),

-- Get all unique partition_date + akkio_id combinations
base_grain AS (
    SELECT DISTINCT
        PARTITION_DATE,
        AKKIO_ID
    FROM {{ ref('vizio_daily_fact_content_detail') }}
)

SELECT
    bg.PARTITION_DATE,
    bg.AKKIO_ID,

    -- Weight (fixed at 11 per requirements)
    11 AS INSCAPE_WEIGHT,

    -- Map of title -> count of sessions
    MAP_FROM_ENTRIES(
        COLLECT_LIST(
            STRUCT(tc.TITLE_KEY AS key, tc.SESSION_COUNT AS value)
        )
    ) AS TITLES_WATCHED,

    -- Map of genre -> count of sessions
    MAP_FROM_ENTRIES(
        COLLECT_LIST(
            STRUCT(gc.GENRE_KEY AS key, gc.SESSION_COUNT AS value)
        )
    ) AS GENRES_WATCHED,

    -- Map of network -> count of sessions
    MAP_FROM_ENTRIES(
        COLLECT_LIST(
            STRUCT(nc.NETWORK_KEY AS key, nc.SESSION_COUNT AS value)
        )
    ) AS NETWORKS_WATCHED,

    -- Map of device/category -> count of sessions
    MAP_FROM_ENTRIES(
        COLLECT_LIST(
            STRUCT(dc.DEVICE_KEY AS key, dc.SESSION_COUNT AS value)
        )
    ) AS INPUT_DEVICES_USED,

    -- Map of app service -> count of sessions
    MAP_FROM_ENTRIES(
        COLLECT_LIST(
            STRUCT(asc.APP_SERVICE_KEY AS key, asc.SESSION_COUNT AS value)
        )
    ) AS APP_SERVICES_USED

FROM base_grain bg
LEFT JOIN title_counts tc
    ON bg.PARTITION_DATE = tc.PARTITION_DATE
    AND bg.AKKIO_ID = tc.AKKIO_ID
LEFT JOIN genre_counts gc
    ON bg.PARTITION_DATE = gc.PARTITION_DATE
    AND bg.AKKIO_ID = gc.AKKIO_ID
LEFT JOIN network_counts nc
    ON bg.PARTITION_DATE = nc.PARTITION_DATE
    AND bg.AKKIO_ID = nc.AKKIO_ID
LEFT JOIN device_counts dc
    ON bg.PARTITION_DATE = dc.PARTITION_DATE
    AND bg.AKKIO_ID = dc.AKKIO_ID
LEFT JOIN app_service_counts asc
    ON bg.PARTITION_DATE = asc.PARTITION_DATE
    AND bg.AKKIO_ID = asc.AKKIO_ID
GROUP BY
    bg.PARTITION_DATE,
    bg.AKKIO_ID
