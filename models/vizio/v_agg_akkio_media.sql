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

WITH
-- Filter source data early for testing
-- Note: Using WHERE clause for filtering (partition pruning applies automatically)
-- For testing, limit to single partition date to reduce data volume
source_data AS (
    SELECT
        PARTITION_DATE,
        AKKIO_ID,
        TITLE,
        GENRE,
        NETWORK,
        INPUT_DEVICE_NAME,
        INPUT_CATEGORY,
        APP_SERVICE
    FROM {{ ref('vizio_daily_fact_content_detail') }}
),

-- Aggregations: Spark will optimize these to minimize actual table scans
-- Since source is partitioned/clustered, multiple GROUP BYs are acceptable

title_counts AS (
    SELECT PARTITION_DATE, AKKIO_ID, COALESCE(TITLE, 'unknown') AS k, CAST(COUNT(*) AS BIGINT) AS v
    FROM source_data
    GROUP BY PARTITION_DATE, AKKIO_ID, COALESCE(TITLE, 'unknown')
),

title_maps AS (
    SELECT PARTITION_DATE, AKKIO_ID, MAP_FROM_ENTRIES(COLLECT_LIST(STRUCT(k, v))) AS TITLES_WATCHED
    FROM title_counts GROUP BY PARTITION_DATE, AKKIO_ID
),

genre_counts AS (
    SELECT PARTITION_DATE, AKKIO_ID, COALESCE(NULLIF(TRIM(g), ''), 'unknown') AS k, CAST(COUNT(*) AS BIGINT) AS v
    FROM source_data
    LATERAL VIEW EXPLODE(SPLIT(COALESCE(GENRE, 'unknown'), ',')) AS g
    GROUP BY PARTITION_DATE, AKKIO_ID, COALESCE(NULLIF(TRIM(g), ''), 'unknown')
),

genre_maps AS (
    SELECT PARTITION_DATE, AKKIO_ID, MAP_FROM_ENTRIES(COLLECT_LIST(STRUCT(k, v))) AS GENRES_WATCHED
    FROM genre_counts GROUP BY PARTITION_DATE, AKKIO_ID
),

network_counts AS (
    SELECT PARTITION_DATE, AKKIO_ID, COALESCE(NETWORK, 'unknown') AS k, CAST(COUNT(*) AS BIGINT) AS v
    FROM source_data
    GROUP BY PARTITION_DATE, AKKIO_ID, COALESCE(NETWORK, 'unknown')
),

network_maps AS (
    SELECT PARTITION_DATE, AKKIO_ID, MAP_FROM_ENTRIES(COLLECT_LIST(STRUCT(k, v))) AS NETWORKS_WATCHED
    FROM network_counts GROUP BY PARTITION_DATE, AKKIO_ID
),

device_counts AS (
    SELECT PARTITION_DATE, AKKIO_ID, COALESCE(INPUT_DEVICE_NAME, INPUT_CATEGORY, 'unknown') AS k, CAST(COUNT(*) AS BIGINT) AS v
    FROM source_data
    GROUP BY PARTITION_DATE, AKKIO_ID, COALESCE(INPUT_DEVICE_NAME, INPUT_CATEGORY, 'unknown')
),

device_maps AS (
    SELECT PARTITION_DATE, AKKIO_ID, MAP_FROM_ENTRIES(COLLECT_LIST(STRUCT(k, v))) AS INPUT_DEVICES_USED
    FROM device_counts GROUP BY PARTITION_DATE, AKKIO_ID
),

app_counts AS (
    SELECT PARTITION_DATE, AKKIO_ID, COALESCE(APP_SERVICE, 'unknown') AS k, CAST(COUNT(*) AS BIGINT) AS v
    FROM source_data
    GROUP BY PARTITION_DATE, AKKIO_ID, COALESCE(APP_SERVICE, 'unknown')
),

app_maps AS (
    SELECT PARTITION_DATE, AKKIO_ID, MAP_FROM_ENTRIES(COLLECT_LIST(STRUCT(k, v))) AS APP_SERVICES_USED
    FROM app_counts GROUP BY PARTITION_DATE, AKKIO_ID
),

base AS (
    SELECT DISTINCT PARTITION_DATE, AKKIO_ID FROM source_data
)

SELECT
    b.PARTITION_DATE,
    b.AKKIO_ID,
    11 AS INSCAPE_WEIGHT,
    t.TITLES_WATCHED,
    g.GENRES_WATCHED,
    n.NETWORKS_WATCHED,
    d.INPUT_DEVICES_USED,
    a.APP_SERVICES_USED
FROM base b
LEFT JOIN title_maps t USING (PARTITION_DATE, AKKIO_ID)
LEFT JOIN genre_maps g USING (PARTITION_DATE, AKKIO_ID)
LEFT JOIN network_maps n USING (PARTITION_DATE, AKKIO_ID)
LEFT JOIN device_maps d USING (PARTITION_DATE, AKKIO_ID)
LEFT JOIN app_maps a USING (PARTITION_DATE, AKKIO_ID)
