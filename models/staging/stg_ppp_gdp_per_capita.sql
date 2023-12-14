{{ config(materialized='view')}}

WITH ppp_gdp_per_capita_stg1 AS (

    SELECT LEFT(ACountryTerritory, LENGTH(ACountryTerritory) - 2) AS country,
        {{ format_numeric_cols('AIMF56') }} AS ppp_gdp_per_capita
    FROM {{ source('google-sheets', 'ppp_gdp_per_capita') }}


)

SELECT * 
FROM ppp_gdp_per_capita_stg1
WHERE ppp_gdp_per_capita IS NOT NULL
