{{ config(materialized='table')}}

-- Combine all views

WITH stg_hdi AS (    
    
    SELECT *
    FROM {{ ref('stg_hdi') }}    

),

stg_nominal_gdp_per_capita AS (

    SELECT * 
    FROM {{ ref('stg_nominal_gdp_per_capita') }}

),

stg_population AS (

    SELECT * 
    FROM {{ ref('stg_population') }}

),

stg_ppp_gdp_per_capita AS (

    SELECT *
    FROM {{ ref('stg_ppp_gdp_per_capita') }}

)

SELECT stg_nominal_gdp_per_capita.country, stg_hdi.hdi, stg_nominal_gdp_per_capita.nominal_gdp_per_capita, 
stg_population.population, stg_ppp_gdp_per_capita.ppp_gdp_per_capita
FROM stg_nominal_gdp_per_capita
LEFT JOIN stg_hdi
ON stg_nominal_gdp_per_capita.country = stg_hdi.country
LEFT JOIN stg_population
ON stg_nominal_gdp_per_capita.country = stg_population.country
LEFT JOIN stg_ppp_gdp_per_capita
ON stg_nominal_gdp_per_capita.country = stg_ppp_gdp_per_capita.country