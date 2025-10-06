{% macro test_recent_data(model, date_column, max_days_old=7) %}

-- Custom test to ensure model has recent data
-- Fails if the most recent data is older than max_days_old

SELECT 
    '{{ model }}' AS model_name,
    MAX({{ date_column }}) AS most_recent_date,
    CURRENT_DATE() AS current_date,
    DATEDIFF(DAY, MAX({{ date_column }}), CURRENT_DATE()) AS days_since_last_data
FROM {{ model }}
HAVING DATEDIFF(DAY, MAX({{ date_column }}), CURRENT_DATE()) > {{ max_days_old }}

{% endmacro %}

