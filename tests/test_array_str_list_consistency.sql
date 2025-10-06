-- Test that ARRAY columns and STR_LIST columns have consistent data
-- Validates data transformation logic in summary tables

{{ config(severity='warn') }}

WITH content_checks AS (
    SELECT 
        PARTITION_DATE,
        TV_ID,
        'network' AS column_type,
        size(NETWORK_ARRAY) AS array_size,
        size(split(NETWORK_STR_LIST, '\\|')) AS str_list_size
    FROM {{ ref('vizio_daily_fact_content_summary') }}
    WHERE NETWORK_STR_LIST IS NOT NULL AND size(NETWORK_ARRAY) > 0
    
    UNION ALL
    
    SELECT 
        PARTITION_DATE,
        TV_ID,
        'title' AS column_type,
        size(TITLE_ARRAY) AS array_size,
        size(split(TITLE_STR_LIST, '\\|')) AS str_list_size
    FROM {{ ref('vizio_daily_fact_content_summary') }}
    WHERE TITLE_STR_LIST IS NOT NULL AND size(TITLE_ARRAY) > 0
)
SELECT 
    PARTITION_DATE,
    TV_ID,
    column_type,
    array_size,
    str_list_size
FROM content_checks
WHERE array_size != str_list_size
LIMIT 100

