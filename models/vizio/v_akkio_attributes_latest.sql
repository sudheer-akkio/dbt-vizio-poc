{{ config(
    materialized='table',
    post_hook=[    
        "alter table {{this}} cluster by (PARTITION_DATE, TV_ID)", 
    ]
)}}

/*
    Vizio Attributes Latest Model
    
    Purpose: Provides the latest demographic and household attributes from Experian 
    for each TV ID. This model decodes household composition data into categorical 
    columns for audience segmentation and targeting use cases.
    
    Source: akkio.akkio_common.mac_vizio_synthetic
    Keys: AKKIO_ID, TV_ID, AKKIO_HH_ID (all contain same value, optimized copies)
    
    Data Structure:
    - Source data contains HOUSEHOLD-LEVEL counts (not individual binary flags)
    - Example: demo_male_25_34 = 2 means there are 2 males aged 25-34 in household
    - Each TV_ID (HASH) represents ONE unique household 
    - Source data is already deduplicated at the HASH level
    - No additional deduplication logic required
    
    Decoded Attributes:
    - Gender: Single column (Male/Female) - presence of any male/female in household
    - Age: Median of age range bucket - based on first matching age group
    - Education: Single categorical value - highest level indicated
    - Ethnicity: Single categorical value - first matching ethnicity
    - Income: Median of income range bracket
    - Other household attributes as Y/N flags (presence/absence)
    
    Note: All comparisons use >= 1 to handle count-based data correctly
*/

-- Optimized: AKKIO_ID, TV_ID, and AKKIO_HH_ID all reference AKKIO_ID
-- These are separate columns but contain the same value for downstream compatibility

WITH source_attributes AS (
    SELECT * FROM {{ source('vizio_poc_share', 'mk_akkio_experian_demo') }}
),

attributes_decoded AS (
    SELECT
        -- Primary Keys (all reference same ID for optimization)
        HASH AS AKKIO_ID,
        HASH AS TV_ID,
        HASH AS AKKIO_HH_ID,
        
        -- Temporal
        CURRENT_DATE() AS PARTITION_DATE,
        
        -- Decode Gender (Male/Female based on which demo columns are populated)
        -- Note: Using >= 1 to handle count-based data (household composition)
        CASE 
            WHEN COALESCE(demo_male_18_24, 0) + COALESCE(demo_male_25_34, 0) + COALESCE(demo_male_35_44, 0) + 
                 COALESCE(demo_male_45_54, 0) + COALESCE(demo_male_55_64, 0) + COALESCE(demo_male_65_999, 0) > 0 
            THEN 'Male'
            WHEN COALESCE(demo_female_18_24, 0) + COALESCE(demo_female_25_34, 0) + COALESCE(demo_female_35_44, 0) + 
                 COALESCE(demo_female_45_54, 0) + COALESCE(demo_female_55_64, 0) + COALESCE(demo_female_65_999, 0) > 0 
            THEN 'Female'
            ELSE NULL
        END AS GENDER,
        
        -- Decode Age (lower bound of age range bucket - integer for insights compatibility)
        -- Note: Using >= 1 to handle count-based data (household composition)
        -- Insights expect integer ages, not decimals
        CASE
            WHEN COALESCE(demo_male_18_24, 0) >= 1 OR COALESCE(demo_female_18_24, 0) >= 1 THEN 18
            WHEN COALESCE(demo_male_25_34, 0) >= 1 OR COALESCE(demo_female_25_34, 0) >= 1 THEN 25
            WHEN COALESCE(demo_male_35_44, 0) >= 1 OR COALESCE(demo_female_35_44, 0) >= 1 THEN 35
            WHEN COALESCE(demo_male_45_54, 0) >= 1 OR COALESCE(demo_female_45_54, 0) >= 1 THEN 45
            WHEN COALESCE(demo_male_55_64, 0) >= 1 OR COALESCE(demo_female_55_64, 0) >= 1 THEN 55
            WHEN COALESCE(demo_male_65_999, 0) >= 1 OR COALESCE(demo_female_65_999, 0) >= 1 THEN 65
            ELSE NULL
        END AS AGE,
        
        -- Age Bucket (encoded as integers to match Horizon schema for insights)
        -- 1: 18-24, 2: 25-34, 3: 35-44, 4: 45-54, 5: 55-64, 6: 65-74, 7: 75+
        CASE
            WHEN COALESCE(demo_male_18_24, 0) >= 1 OR COALESCE(demo_female_18_24, 0) >= 1 THEN 1
            WHEN COALESCE(demo_male_25_34, 0) >= 1 OR COALESCE(demo_female_25_34, 0) >= 1 THEN 2
            WHEN COALESCE(demo_male_35_44, 0) >= 1 OR COALESCE(demo_female_35_44, 0) >= 1 THEN 3
            WHEN COALESCE(demo_male_45_54, 0) >= 1 OR COALESCE(demo_female_45_54, 0) >= 1 THEN 4
            WHEN COALESCE(demo_male_55_64, 0) >= 1 OR COALESCE(demo_female_55_64, 0) >= 1 THEN 5
            WHEN COALESCE(demo_male_65_999, 0) >= 1 OR COALESCE(demo_female_65_999, 0) >= 1 THEN 6
            ELSE 7
        END AS AGE_BUCKET,
        
        -- Decode Education Level (highest level indicated)
        CASE 
            WHEN COALESCE(edu_graduate, 0) >= 1 THEN 'Graduate'
            WHEN COALESCE(edu_college, 0) >= 1 THEN 'College'
            WHEN COALESCE(edu_some_college, 0) >= 1 THEN 'Some College'
            WHEN COALESCE(edu_high_school, 0) >= 1 THEN 'High School'
            ELSE NULL
        END AS EDUCATION_LEVEL,
        
        -- Decode Ethnicity
        CASE 
            WHEN COALESCE(ethnicity_african_american, 0) >= 1 THEN 'African American'
            WHEN COALESCE(ethnicity_asian, 0) >= 1 THEN 'Asian'
            WHEN COALESCE(ethnicity_white_non_hispanic, 0) >= 1 THEN 'White Non-Hispanic'
            WHEN COALESCE(ethnicity_hispanic, 0) >= 1 THEN 'Hispanic'
            WHEN COALESCE(ethnicity_middle_eastern, 0) >= 1 THEN 'Middle Eastern'
            WHEN COALESCE(ethnicity_native_american, 0) >= 1 THEN 'Native American'
            WHEN COALESCE(ethnicity_other, 0) >= 1 THEN 'Other'
            WHEN COALESCE(ethnicity_unknown, 0) >= 1 THEN 'Unknown'
            ELSE NULL
        END AS ETHNICITY,
        
        -- Language
        CASE WHEN COALESCE(language_spanish, 0) >= 1 THEN 'Y' ELSE 'N' END AS SPANISH_LANGUAGE,
        
        -- Marital Status
        CASE 
            WHEN COALESCE(marital_status_married, 0) >= 1 THEN 'Married'
            WHEN COALESCE(marital_status_single, 0) >= 1 THEN 'Single'
            ELSE NULL
        END AS MARITAL_STATUS,
        
        -- Home Ownership (numeric for insights compatibility: 1=Owner, 0=Renter, NULL=Unknown)
        CASE
            WHEN COALESCE(home_owner_hh, 0) >= 1 THEN 1
            WHEN COALESCE(home_renter_hh, 0) >= 1 THEN 0
            ELSE NULL
        END AS HOME_OWNERSHIP,
        
        -- Decode Household Income (lower bound of income range in thousands - integer for insights compatibility)
        -- Insights expect integer values for histogram min_bound/max_bound
        CASE
            WHEN COALESCE(income_0_35_hh, 0) >= 1 THEN 0
            WHEN COALESCE(income_35_45_hh, 0) >= 1 THEN 35
            WHEN COALESCE(income_45_55_hh, 0) >= 1 THEN 45
            WHEN COALESCE(income_55_70_hh, 0) >= 1 THEN 55
            WHEN COALESCE(income_70_85_hh, 0) >= 1 THEN 70
            WHEN COALESCE(income_85_100_hh, 0) >= 1 THEN 85
            WHEN COALESCE(income_100_125_hh, 0) >= 1 THEN 100
            WHEN COALESCE(income_125_150_hh, 0) >= 1 THEN 125
            WHEN COALESCE(income_150_200_hh, 0) >= 1 THEN 150
            WHEN COALESCE(income_200_plus_hh, 0) >= 1 THEN 200
            ELSE NULL
        END AS HOUSEHOLD_INCOME_K,
        
        -- Income Bucket (encoded as integers to match Horizon schema for insights)
        -- 1: $0-35K, 2: $35-45K, 3: $45-55K, 4: $55-70K, 5: $70-85K,
        -- 6: $85-100K, 7: $100-125K, 8: $125-150K, 9: $150-200K, 10: $200K+
        CASE
            WHEN COALESCE(income_0_35_hh, 0) >= 1 THEN 1
            WHEN COALESCE(income_35_45_hh, 0) >= 1 THEN 2
            WHEN COALESCE(income_45_55_hh, 0) >= 1 THEN 3
            WHEN COALESCE(income_55_70_hh, 0) >= 1 THEN 4
            WHEN COALESCE(income_70_85_hh, 0) >= 1 THEN 5
            WHEN COALESCE(income_85_100_hh, 0) >= 1 THEN 6
            WHEN COALESCE(income_100_125_hh, 0) >= 1 THEN 7
            WHEN COALESCE(income_125_150_hh, 0) >= 1 THEN 8
            WHEN COALESCE(income_150_200_hh, 0) >= 1 THEN 9
            WHEN COALESCE(income_200_plus_hh, 0) >= 1 THEN 10
            ELSE 11
        END AS INCOME_BUCKET,
        
        -- Household Composition (Y/N flags)
        CASE WHEN COALESCE(babies_0_3_hh, 0) >= 1 THEN 'Y' ELSE 'N' END AS HAS_BABIES_0_3,
        CASE WHEN COALESCE(children_0_18_hh, 0) >= 1 THEN 'Y' ELSE 'N' END AS HAS_CHILDREN_0_18,
        
        -- Adult Household Size (keep as integer)
        COALESCE(adult_hh_size, 0) AS ADULT_HOUSEHOLD_SIZE,
        
        -- Household Mobility (Y/N flags)
        CASE WHEN COALESCE(move_likely_hh, 0) >= 1 THEN 'Y' ELSE 'N' END AS MOVE_LIKELY,
        CASE WHEN COALESCE(move_recent_hh, 0) >= 1 THEN 'Y' ELSE 'N' END AS MOVE_RECENT,
        
        -- Data Quality Indicators (Y/N flags)
        CASE WHEN COALESCE(low_quality, 0) >= 1 THEN 'Y' ELSE 'N' END AS LOW_QUALITY_FLAG,
        CASE WHEN COALESCE(demo_incomplete, 0) >= 1 THEN 'Y' ELSE 'N' END AS DEMO_INCOMPLETE_FLAG
        
    FROM source_attributes
    WHERE HASH IS NOT NULL
)

-- OLD DEDUP LOGIC
--
-- TODO: If source table has timestamp/ingestion_date, replace this with temporal ordering
-- latest_records AS (
--     SELECT 
--         *,
--         ROW_NUMBER() OVER (
--             PARTITION BY TV_ID 
--             ORDER BY 
--                 -- Prefer records with complete demographic data
--                 CASE WHEN GENDER IS NULL THEN 1 ELSE 0 END,
--                 CASE WHEN AGE IS NULL THEN 1 ELSE 0 END,
--                 CASE WHEN HOUSEHOLD_INCOME_K IS NULL THEN 1 ELSE 0 END,
--                 CASE WHEN EDUCATION_LEVEL IS NULL THEN 1 ELSE 0 END,
--                 CASE WHEN ETHNICITY IS NULL THEN 1 ELSE 0 END,
                
--                 -- Prefer higher-value households (business logic)
--                 HOUSEHOLD_INCOME_K DESC NULLS LAST,
--                 ADULT_HOUSEHOLD_SIZE DESC NULLS LAST,
                
--                 -- Education hierarchy (Graduate > College > Some College > HS)
--                 CASE EDUCATION_LEVEL
--                     WHEN 'Graduate' THEN 4
--                     WHEN 'College' THEN 3
--                     WHEN 'Some College' THEN 2
--                     WHEN 'High School' THEN 1
--                     ELSE 0
--                 END DESC,
                
--                 -- Data quality (prefer non-flagged records)
--                 CASE WHEN LOW_QUALITY_FLAG = 'Y' THEN 1 ELSE 0 END,
--                 CASE WHEN DEMO_INCOMPLETE_FLAG = 'Y' THEN 1 ELSE 0 END,
                
--                 -- Final tiebreaker for full determinism
--                 AKKIO_ID  -- Ensures consistent results even for identical households
--         ) AS row_num
--     FROM attributes_decoded
-- )

SELECT
    -- Keys and Temporal
    PARTITION_DATE,
    AKKIO_ID,
    TV_ID,
    AKKIO_HH_ID,
    
    -- Demographics (Decoded)
    GENDER,
    AGE,
    AGE_BUCKET,

    -- Geographic Attributes (not available in Vizio data - required by audience service)
    NULL AS STATE,
    NULL AS ZIP11,
    NULL AS COUNTY_NAME,

    -- Socioeconomic Attributes (Decoded)
    EDUCATION_LEVEL,
    ETHNICITY,
    SPANISH_LANGUAGE,
    MARITAL_STATUS,
    HOME_OWNERSHIP,
    HOME_OWNERSHIP AS HOMEOWNER,  -- Alias for audience queries

    -- Income (Decoded)
    HOUSEHOLD_INCOME_K,
    HOUSEHOLD_INCOME_K AS INCOME,  -- Alias for audience queries
    INCOME_BUCKET,

    -- Financial Attributes (not available in Vizio data - required by audience service)
    NULL AS NET_WORTH,
    
    -- Household Composition
    HAS_BABIES_0_3,
    HAS_CHILDREN_0_18,
    ADULT_HOUSEHOLD_SIZE,
    
    -- Household Mobility
    MOVE_LIKELY,
    MOVE_RECENT,
    
    -- Data Quality
    LOW_QUALITY_FLAG,
    DEMO_INCOMPLETE_FLAG,
    
    -- Processing Metadata
    CURRENT_TIMESTAMP() AS DBT_UPDATED_AT

FROM attributes_decoded
