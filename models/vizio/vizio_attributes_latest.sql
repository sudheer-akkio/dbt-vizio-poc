{{ config(
    materialized='table',
    post_hook=[    
        "alter table {{this}} cluster by (match_date, tvid)", 
    ]
)}}

/*
    Vizio Attributes Latest Model
    
    Purpose: Provides the latest demographic and household attributes from Experian 
    for each TV ID. This model decodes one-hot encoded attributes into categorical 
    columns for audience segmentation and targeting use cases.
    
    Source: akkio.akkio_common.mac_vizio_synethic (to be updated)
    Key: tvid - Links to all other Vizio fact tables for audience analytics
    
    Decoded Attributes:
    - Gender: Single column (Male/Female)
    - Age: Median of age range bucket
    - Education: Single categorical value
    - Ethnicity: Single categorical value
    - Income: Median of income range bucket
    - Other household attributes as Y/N flags
*/

WITH source_attributes AS (
    SELECT * FROM {{ source('akkio_common', 'mac_vizio_synthetic') }}
),

attributes_decoded AS (
    SELECT
        -- Primary Key
        AKKIO_ID AS TVID,
        
        -- Temporal
        match_date AS MATCH_DATE,
        DATE(match_date) AS PARTITION_DATE,
        
        -- Decode Gender (Male/Female based on which demo columns are populated)
        CASE 
            WHEN COALESCE(demo_male_18_24, 0) + COALESCE(demo_male_25_34, 0) + COALESCE(demo_male_35_44, 0) + 
                 COALESCE(demo_male_45_54, 0) + COALESCE(demo_male_55_64, 0) + COALESCE(demo_male_65_999, 0) > 0 
            THEN 'Male'
            WHEN COALESCE(demo_female_18_24, 0) + COALESCE(demo_female_25_34, 0) + COALESCE(demo_female_35_44, 0) + 
                 COALESCE(demo_female_45_54, 0) + COALESCE(demo_female_55_64, 0) + COALESCE(demo_female_65_999, 0) > 0 
            THEN 'Female'
            ELSE NULL
        END AS GENDER,
        
        -- Decode Age (Median of age range bucket) - using standard buckets
        CASE 
            WHEN COALESCE(demo_male_18_24, 0) = 1 OR COALESCE(demo_female_18_24, 0) = 1 THEN 21
            WHEN COALESCE(demo_male_25_34, 0) = 1 OR COALESCE(demo_female_25_34, 0) = 1 THEN 29.5
            WHEN COALESCE(demo_male_35_44, 0) = 1 OR COALESCE(demo_female_35_44, 0) = 1 THEN 39.5
            WHEN COALESCE(demo_male_45_54, 0) = 1 OR COALESCE(demo_female_45_54, 0) = 1 THEN 49.5
            WHEN COALESCE(demo_male_55_64, 0) = 1 OR COALESCE(demo_female_55_64, 0) = 1 THEN 59.5
            WHEN COALESCE(demo_male_65_999, 0) = 1 OR COALESCE(demo_female_65_999, 0) = 1 THEN 72.5
            ELSE NULL
        END AS AGE,
        
        -- Decode Age Bucket (keep as label for reference)
        CASE 
            WHEN COALESCE(demo_male_18_24, 0) = 1 OR COALESCE(demo_female_18_24, 0) = 1 THEN '18-24'
            WHEN COALESCE(demo_male_25_34, 0) = 1 OR COALESCE(demo_female_25_34, 0) = 1 THEN '25-34'
            WHEN COALESCE(demo_male_35_44, 0) = 1 OR COALESCE(demo_female_35_44, 0) = 1 THEN '35-44'
            WHEN COALESCE(demo_male_45_54, 0) = 1 OR COALESCE(demo_female_45_54, 0) = 1 THEN '45-54'
            WHEN COALESCE(demo_male_55_64, 0) = 1 OR COALESCE(demo_female_55_64, 0) = 1 THEN '55-64'
            WHEN COALESCE(demo_male_65_999, 0) = 1 OR COALESCE(demo_female_65_999, 0) = 1 THEN '65+'
            ELSE NULL
        END AS AGE_BUCKET,
        
        -- Decode Education Level (highest level indicated)
        CASE 
            WHEN COALESCE(edu_graduate, 0) = 1 THEN 'Graduate'
            WHEN COALESCE(edu_college, 0) = 1 THEN 'College'
            WHEN COALESCE(edu_some_college, 0) = 1 THEN 'Some College'
            WHEN COALESCE(edu_high_school, 0) = 1 THEN 'High School'
            ELSE NULL
        END AS EDUCATION_LEVEL,
        
        -- Decode Ethnicity
        CASE 
            WHEN COALESCE(ethnicity_african_american, 0) = 1 THEN 'African American'
            WHEN COALESCE(ethnicity_asian, 0) = 1 THEN 'Asian'
            WHEN COALESCE(ethnicity_white_non_hispanic, 0) = 1 THEN 'White Non-Hispanic'
            WHEN COALESCE(ethnicity_hispanic, 0) = 1 THEN 'Hispanic'
            WHEN COALESCE(ethnicity_middle_eastern, 0) = 1 THEN 'Middle Eastern'
            WHEN COALESCE(ethnicity_native_american, 0) = 1 THEN 'Native American'
            WHEN COALESCE(ethnicity_other, 0) = 1 THEN 'Other'
            WHEN COALESCE(ethnicity_unknown, 0) = 1 THEN 'Unknown'
            ELSE NULL
        END AS ETHNICITY,
        
        -- Language
        CASE WHEN COALESCE(language_spanish, 0) = 1 THEN 'Y' ELSE 'N' END AS SPANISH_LANGUAGE,
        
        -- Marital Status
        CASE 
            WHEN COALESCE(marital_status_married, 0) = 1 THEN 'Married'
            WHEN COALESCE(marital_status_single, 0) = 1 THEN 'Single'
            ELSE NULL
        END AS MARITAL_STATUS,
        
        -- Home Ownership
        CASE 
            WHEN COALESCE(home_owner_hh, 0) = 1 THEN 'Owner'
            WHEN COALESCE(home_renter_hh, 0) = 1 THEN 'Renter'
            ELSE NULL
        END AS HOME_OWNERSHIP,
        
        -- Decode Household Income (Median of income range in thousands)
        CASE 
            WHEN COALESCE(income_0_35_hh, 0) = 1 THEN 17.5
            WHEN COALESCE(income_35_45_hh, 0) = 1 THEN 40
            WHEN COALESCE(income_45_55_hh, 0) = 1 THEN 50
            WHEN COALESCE(income_55_70_hh, 0) = 1 THEN 62.5
            WHEN COALESCE(income_70_85_hh, 0) = 1 THEN 77.5
            WHEN COALESCE(income_85_100_hh, 0) = 1 THEN 92.5
            WHEN COALESCE(income_100_125_hh, 0) = 1 THEN 112.5
            WHEN COALESCE(income_125_150_hh, 0) = 1 THEN 137.5
            WHEN COALESCE(income_150_200_hh, 0) = 1 THEN 175
            WHEN COALESCE(income_200_plus_hh, 0) = 1 THEN 250
            ELSE NULL
        END AS HOUSEHOLD_INCOME_K,
        
        -- Decode Income Bracket (keep as label for reference)
        CASE 
            WHEN COALESCE(income_0_35_hh, 0) = 1 THEN '$0-35K'
            WHEN COALESCE(income_35_45_hh, 0) = 1 THEN '$35-45K'
            WHEN COALESCE(income_45_55_hh, 0) = 1 THEN '$45-55K'
            WHEN COALESCE(income_55_70_hh, 0) = 1 THEN '$55-70K'
            WHEN COALESCE(income_70_85_hh, 0) = 1 THEN '$70-85K'
            WHEN COALESCE(income_85_100_hh, 0) = 1 THEN '$85-100K'
            WHEN COALESCE(income_100_125_hh, 0) = 1 THEN '$100-125K'
            WHEN COALESCE(income_125_150_hh, 0) = 1 THEN '$125-150K'
            WHEN COALESCE(income_150_200_hh, 0) = 1 THEN '$150-200K'
            WHEN COALESCE(income_200_plus_hh, 0) = 1 THEN '$200K+'
            ELSE NULL
        END AS INCOME_BRACKET,
        
        -- Household Composition (Y/N flags)
        CASE WHEN COALESCE(babies_0_3_hh, 0) = 1 THEN 'Y' ELSE 'N' END AS HAS_BABIES_0_3,
        CASE WHEN COALESCE(children_0_18_hh, 0) = 1 THEN 'Y' ELSE 'N' END AS HAS_CHILDREN_0_18,
        
        -- Adult Household Size (keep as integer)
        COALESCE(adult_hh_size, 0) AS ADULT_HOUSEHOLD_SIZE,
        
        -- Household Mobility (Y/N flags)
        CASE WHEN COALESCE(move_likely_hh, 0) = 1 THEN 'Y' ELSE 'N' END AS MOVE_LIKELY,
        CASE WHEN COALESCE(move_recent_hh, 0) = 1 THEN 'Y' ELSE 'N' END AS MOVE_RECENT,
        
        -- Data Quality Indicators (Y/N flags)
        CASE WHEN COALESCE(low_quality, 0) = 1 THEN 'Y' ELSE 'N' END AS LOW_QUALITY_FLAG,
        CASE WHEN COALESCE(demo_incomplete, 0) = 1 THEN 'Y' ELSE 'N' END AS DEMO_INCOMPLETE_FLAG,
        
        -- Metadata
        CURRENT_TIMESTAMP() AS DBT_UPDATED_AT
        
    FROM source_attributes
    WHERE AKKIO_ID IS NOT NULL
),

-- Get only the most recent match for each TV ID
latest_records AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY TVID 
            ORDER BY MATCH_DATE DESC, DBT_UPDATED_AT DESC
        ) AS row_num
    FROM attributes_decoded
)

SELECT
    -- Keys and Temporal
    PARTITION_DATE,
    TVID,
    MATCH_DATE,
    
    -- Demographics (Decoded)
    GENDER,
    AGE,
    AGE_BUCKET,
    
    -- Socioeconomic Attributes (Decoded)
    EDUCATION_LEVEL,
    ETHNICITY,
    SPANISH_LANGUAGE,
    MARITAL_STATUS,
    HOME_OWNERSHIP,
    
    -- Income (Decoded)
    HOUSEHOLD_INCOME_K,
    INCOME_BRACKET,
    
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
    
    -- Metadata
    DBT_UPDATED_AT

FROM latest_records
WHERE row_num = 1
