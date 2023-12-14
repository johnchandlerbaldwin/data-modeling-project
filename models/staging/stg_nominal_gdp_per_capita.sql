{{ config(materialized='view')}}

WITH nominal_gdp_per_capita_stg1 AS (

    SELECT ACountryTerritory AS country, AUNRegion AS region,
        {{ format_numeric_cols('AIMF45') }} AS nominal_gdp_per_capita
    FROM {{ source('google-sheets', 'nominal_gdp_per_capita') }}


)

SELECT * 
FROM nominal_gdp_per_capita_stg1
WHERE nominal_gdp_per_capita IS NOT NULL
