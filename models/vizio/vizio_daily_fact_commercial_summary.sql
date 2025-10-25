{{ config(
    alias='PV_VIZIO_DAILY_FACT_COMMERCIAL_SUMMARY',
    materialized='incremental',
    unique_key=['PARTITION_DATE', 'AKKIO_ID'],
    incremental_strategy='merge',
    partition_by="PARTITION_DATE",
)}}

WITH 
detail AS (
    SELECT 
        PARTITION_DATE,
        AKKIO_ID,
        TIMEZONE,
        ZIP_CODE,
        DMA,
        CREATIVE_ID,
        BRAND_NAME,
        AD_TITLE,
        COMMERCIAL_CATEGORY,
        INPUT_CATEGORY,
        INPUT_DEVICE_NAME,
        APP_SERVICE,
        AD_LENGTH
    FROM {{ ref('vizio_daily_fact_commercial_detail') }}
    {% if is_incremental() %}
        WHERE PARTITION_DATE > (SELECT MAX(PARTITION_DATE) FROM {{ this }})
    {% endif %}
)
SELECT
    PARTITION_DATE,
    PARTITION_DATE AS VIEWED_DATE,
    AKKIO_ID,
    MAX(TIMEZONE) AS TIMEZONE,
    max(ZIP_CODE) AS ZIP_CODE,
    max(DMA) AS DMA,
	string_agg(DISTINCT CREATIVE_ID, ',') AS CRETIVE_ID_STR_LIST,
    string_agg(DISTINCT BRAND_NAME, ',') AS BRAND_NAME_STR_LIST,
    string_agg(DISTINCT AD_TITLE, ',') AS AD_TITLE_STR_LIST,
    string_agg(DISTINCT COMMERCIAL_CATEGORY, ',') AS COMMERCIAL_CATEGORY_STR_LIST,
    string_agg(DISTINCT INPUT_CATEGORY, ',') AS INPUT_CATEGORY_STR_LIST,
    string_agg(DISTINCT INPUT_DEVICE_NAME, ',') AS INPUT_DEVICE_STR_LIST,
    string_agg(DISTINCT APP_SERVICE, ',') AS APP_SERVICE_STR_LIST,
    COUNT(*) AS TOTAL_AD_VIEWS,
    SUM(AD_LENGTH) AS TOTAL_AD_SECONDS
FROM
    detail
GROUP BY
    PARTITION_DATE, AKKIO_ID

