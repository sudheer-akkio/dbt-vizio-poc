{{ config(
    materialized='table',
    post_hook=[    
        "alter table {{this}} cluster by (PARTITION_DATE, AKKIO_ID)", 
    ]
)}}

/*
    Vizio Individual Aggregation Table

    Purpose: Individual-level aggregation of demographic attributes for analytics.
    Source: v_akkio_attributes_latest
    Grain: One row per AKKIO_ID (individual device/person)

    MAIDS, IPS, EMAILS, PHONES: Placeholders for potential future enrichment from additional data sources
*/

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
    COALESCE(attr.ETHNICITY, 'Unknown') AS ETHNICITY_PREDICTION,
    COALESCE(attr.EDUCATION_LEVEL, 'Unknown') as EDUCATION,
    COALESCE(attr.MARITAL_STATUS, 'Unknown') as MARITAL_STATUS,

    COALESCE(attr.STATE, 'Unknown') AS STATE,

    -- Household-level attributes (needed for audience queries - same as Horizon's V_AGG_BLU_IND)
    attr.HOME_OWNERSHIP AS HOMEOWNER,
    attr.HOUSEHOLD_INCOME_K * 1000 AS INCOME,
    attr.INCOME_BUCKET,
    attr.ZIP11 AS ZIP_CODE,

    -- Contact identifiers (counts, not arrays, for insights compatibility)
    0 AS MAIDS,
    0 AS IPS,
    0 AS EMAILS,
    0 AS PHONES,

    -- Temporal
    attr.PARTITION_DATE

FROM {{ ref('v_akkio_attributes_latest') }} attr
