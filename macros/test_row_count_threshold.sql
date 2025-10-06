{% macro test_row_count_threshold(model, minimum_rows=1) %}

-- Custom test to ensure model has minimum number of rows
-- Usage in schema.yml:
--   data_tests:
--     - dbt_utils.expression_is_true:
--         expression: "{{ test_row_count_threshold(this, 1000) }}"

SELECT 
    '{{ model }}' AS model_name,
    COUNT(*) AS actual_rows,
    {{ minimum_rows }} AS minimum_required_rows
FROM {{ model }}
HAVING COUNT(*) < {{ minimum_rows }}

{% endmacro %}

