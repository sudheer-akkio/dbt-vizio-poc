-- Test to ensure vizio_daily_fact_standard_summary has data
-- This validates the model builds successfully and produces results

{{ config(severity='error') }}

SELECT 
    'vizio_daily_fact_standard_summary' AS model_name,
    COUNT(*) AS row_count
FROM {{ ref('vizio_daily_fact_standard_summary') }}
HAVING COUNT(*) = 0

