
-- Remove , from number columns
-- If field does not contain numbers for number columns, replace with null

{% macro format_numeric_cols(column_name) %}

    CAST(CASE WHEN CAST({{ column_name }} AS STRING)  = '..' THEN NULL 
              WHEN CAST({{ column_name }} AS STRING) = '—' THEN NULL
              WHEN CAST({{ column_name }} AS STRING) = 'Estimate' THEN NULL
              WHEN CAST({{ column_name }} AS STRING) LIKE '%*%' THEN NULL
              ELSE REPLACE(CAST({{ column_name }} AS STRING), ',', '') END AS DECIMAL)

{% endmacro %}