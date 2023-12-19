{{ config(materialized='view')}}

WITH nominal_gdp_per_capita_stg1 AS (

    SELECT ACountryTerritory AS country,
        {{ format_numeric_cols('AIMF45') }} AS nominal_gdp_per_capita
    FROM {{ source('google-sheets', 'nominal_gdp_per_capita') }}
),
nominal_gdp_per_capita_stg2 AS (

    SELECT CASE WHEN country = 'DR Congo' THEN 'Democratic Republic of the Congo' ELSE country END AS country,
    nominal_gdp_per_capita
    FROM nominal_gdp_per_capita_stg1
)

SELECT * 
FROM nominal_gdp_per_capita_stg2
WHERE nominal_gdp_per_capita IS NOT NULL
AND country IS NOT NULL
ORDER BY country