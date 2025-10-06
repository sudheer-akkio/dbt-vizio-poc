-- Test to ensure vizio_daily_fact_content_detail has data
-- This validates the model builds successfully and produces results

{{ config(severity='error') }}

SELECT 
    'vizio_daily_fact_content_detail' AS model_name,
    COUNT(*) AS row_count
FROM {{ ref('vizio_daily_fact_content_detail') }}
HAVING COUNT(*) = 0

