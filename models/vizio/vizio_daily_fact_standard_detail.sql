{{ config(
    alias='PV_VIZIO_DAILY_FACT_STANDARD_DETAIL',
    materialized='incremental',
    post_hook=[    
        "alter table {{this}} cluster by (activity_date, akkio_id)", 
    ]
)}}

WITH 
ipage AS (
    SELECT *,
	ROW_NUMBER() OVER (PARTITION BY date_partition, hash ORDER BY ts_start) as row_num,
	SUM(DATEDIFF(SECOND, i.ts_start, i.ts_end)) OVER (PARTITION BY date_partition, hash) as all_total_seconds
    FROM {{ source('vizio_poc_share', 'production_r2081_ipage')}} as i
	WHERE i.hash IS NOT NULL
    {% if var('start_date', None) and var('end_date', None) %}
        -- Batch processing mode: use --vars '{"start_date": "2024-01-01", "end_date": "2024-01-31"}'
        AND date_partition BETWEEN '{{ var("start_date") }}' AND '{{ var("end_date") }}'
    {% elif is_incremental() %}
        -- Normal incremental mode: process only new data
        AND date_partition > (SELECT MAX(PARTITION_DATE) FROM {{ this }})
    {% endif %}
),
timezone_mapping AS (SELECT * FROM {{ source('vizio_poc_share', 'mk_akkio_tvtimezone_mapping') }})
SELECT 
    i.date_partition AS PARTITION_DATE,
    i.date_partition AS ACTIVITY_DATE,
    i.hash AS AKKIO_ID,
    tz.timezone AS TIMEZONE,
    all_total_seconds AS TOTAL_SECONDS,
    i.city AS CITY,
    i.iso_state AS STATE_CODE,
    i.dma AS DMA,
    i.zipcode AS ZIP_CODE
FROM ipage i
LEFT JOIN timezone_mapping tz
    ON i.hash = tz.hash
WHERE i.hash IS NOT NULL
AND i.row_num = 1

