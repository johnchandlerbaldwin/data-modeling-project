{{ config(materialized='view')}}

WITH population_stg1 AS (

    SELECT REGEXP_REPLACE(ACountryDependency, r'\s*\([^)]*\)$', '') AS country,
        {{ format_numeric_cols('APopulation') }} AS population
    FROM {{ source('google-sheets', 'population') }}
),
population_stg2 AS (
    SELECT CASE WHEN country = 'DR Congo' THEN 'Democratic Republic of the Congo' ELSE country END AS country,
    population
    FROM population_stg1
)

SELECT * 
FROM population_stg2
ORDER BY country ASC