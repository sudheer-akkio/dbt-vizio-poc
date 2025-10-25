{{ config(
    materialized='table',
    post_hook=[    
        "alter table {{this}} cluster by (PARTITION_DATE, AKKIO_ID)", 
    ]
)}}

/*
    Vizio Individual Aggregation Table
    
    Purpose: Individual-level aggregation of demographic attributes for analytics.
    Source: v_akkio_attributes_latest + IP aggregation from activity tables
    Grain: One row per AKKIO_ID (individual device/person)
    
    IPS: Aggregated from all detail tables (content, commercial, standard activity, campaign attribution)
    MAIDS, EMAILS, PHONES: Placeholders for potential future enrichment from additional data sources
*/

WITH 
-- Collect all unique IPs from content viewing activity
content_ips AS (
    SELECT 
        AKKIO_ID,
        HASHED_IP
    FROM {{ ref('vizio_daily_fact_content_detail') }}
    WHERE HASHED_IP IS NOT NULL
),

-- Collect all unique IPs from commercial viewing activity
commercial_ips AS (
    SELECT 
        AKKIO_ID,
        HASHED_IP
    FROM {{ ref('vizio_daily_fact_commercial_detail') }}
    WHERE HASHED_IP IS NOT NULL
),

-- Collect all unique IPs from standard device activity
standard_ips AS (
    SELECT 
        AKKIO_ID,
        HASHED_IP
    FROM {{ ref('vizio_daily_fact_standard_detail') }}
    WHERE HASHED_IP IS NOT NULL
),
-- Union all IP sources and aggregate by AKKIO_ID
aggregated_ips AS (
    SELECT 
        AKKIO_ID,
        COLLECT_SET(HASHED_IP) AS IPS_ARRAY
    FROM (
        SELECT AKKIO_ID, HASHED_IP FROM content_ips
        UNION ALL
        SELECT AKKIO_ID, HASHED_IP FROM commercial_ips
        UNION ALL
        SELECT AKKIO_ID, HASHED_IP FROM standard_ips    )
    GROUP BY AKKIO_ID
)

SELECT
    -- Primary Keys
    attr.AKKIO_ID,
    attr.AKKIO_HH_ID,
    
    -- Weight (fixed at 11 per requirements)
    11 AS WEIGHT,
    
    -- Demographics (convert NULL to 'UNDETERMINED' to match Horizon schema for insights compatibility)
    COALESCE(attr.GENDER, 'UNDETERMINED') AS GENDER,
    attr.AGE,
    attr.AGE_BUCKET,
    attr.ETHNICITY,
    attr.EDUCATION_LEVEL,
    attr.MARITAL_STATUS,

    -- Household-level attributes (needed for audience queries - same as Horizon's V_AGG_BLU_IND)
    attr.HOME_OWNERSHIP AS HOMEOWNER,
    attr.HOUSEHOLD_INCOME_K * 1000 AS INCOME,
    attr.INCOME_BUCKET,
    attr.ZIP11 AS ZIP_CODE,

    -- Contact identifiers (counts, not arrays, for insights compatibility)
    0 AS MAIDS,
    COALESCE(SIZE(ips.IPS_ARRAY), 0) AS IPS,
    0 AS EMAILS,
    0 AS PHONES,

    -- Temporal
    attr.PARTITION_DATE

FROM {{ ref('v_akkio_attributes_latest') }} attr
LEFT JOIN aggregated_ips ips
    ON attr.AKKIO_ID = ips.AKKIO_ID
