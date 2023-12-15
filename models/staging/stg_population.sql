{{ config(materialized='view')}}

WITH population_stg1 AS (

    SELECT REGEXP_REPLACE(ACountryDependency, r'\s*\([^)]*\)$', '') AS country,
        {{ format_numeric_cols('APopulation') }} AS population
    FROM {{ source('google-sheets', 'population') }}


)

SELECT * 
FROM population_stg1
