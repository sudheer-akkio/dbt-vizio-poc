-- Test that all markets in campaign attribution have valid population data
-- Ensures market-level metrics can be calculated correctly

{{ config(severity='error') }}

SELECT 
    MARKET,
    COUNT(*) AS impressions,
    COUNT(DISTINCT TV_ID) AS unique_devices,
    MAX(MARKET_TV_POPULATION) AS tv_population
FROM {{ ref('vizio_campaign_attribution') }}
WHERE MARKET IS NOT NULL
GROUP BY MARKET
HAVING MAX(MARKET_TV_POPULATION) IS NULL 
    OR MAX(MARKET_TV_POPULATION) <= 0

