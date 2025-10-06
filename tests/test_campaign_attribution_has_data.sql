-- Test to ensure vizio_campaign_attribution has data
-- This validates the model builds successfully and produces results

{{ config(severity='error') }}

SELECT 
    'vizio_campaign_attribution' AS model_name,
    COUNT(*) AS row_count
FROM {{ ref('vizio_campaign_attribution') }}
HAVING COUNT(*) = 0

