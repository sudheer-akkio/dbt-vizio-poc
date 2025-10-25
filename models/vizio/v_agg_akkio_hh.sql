{{ config(
    materialized='table',
    post_hook=[    
        "alter table {{this}} cluster by (PARTITION_DATE, AKKIO_HH_ID)", 
    ]
)}}

/*
    Vizio Household Aggregation Table
    
    Purpose: Household-level aggregation of demographic attributes for analytics.
    Source: v_akkio_attributes_latest
    Grain: One row per AKKIO_HH_ID (household)
    
    Note: Since AKKIO_HH_ID = AKKIO_ID in the source, this is currently 1:1,
    but structured for future scenarios where multiple individuals may share a household.
*/

SELECT
    -- Primary Key
    AKKIO_HH_ID,
    
    -- Weight (fixed at 11 per requirements)
    11 AS HH_WEIGHT,
    
    -- Home Ownership
    HOME_OWNERSHIP AS HOMEOWNER,
    
    -- Household Income
    HOUSEHOLD_INCOME_K * 1000 AS INCOME,
    CASE
        WHEN HAS_CHILDREN_0_18 = 'Y' THEN 1
        ELSE 0
    END AS PRESENCE_OF_CHILDREN,
    INCOME_BUCKET,
    
    -- Temporal
    PARTITION_DATE

FROM {{ ref('v_akkio_attributes_latest') }}

