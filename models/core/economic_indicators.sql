{{ config(materialized='table')}}

-- Combine all views

SELECT * FROM stg_hdi

/*
WITH hdi AS (    
    
    SELECT *
    FROM {{ ref('stg_hdi') }}    

),

nominal_gdp_per_capita AS (

    SELECT * 
    FROM {{ ref('stg_nominal_gdp_per_capita') }}

),

population AS (

    SELECT * 
    FROM {{ ref('stg_population') }}

),

ppp_gdp_per_capita AS (

    SELECT *
    FROM {{ ref('stg_ppp_gdp_per_capita') }}

)

SELECT nominal_gdp_per_capita.country, hdi, nominal_gdp_per_capita, population, ppp_gdp_per_capita
FROM nominal_gdp_per_capita
INNER JOIN hdi
ON nominal_gdp_per_capita.country = hdi.country
INNER JOIN population
ON nominal_gdp_per_capita.country = population.country
INNER JOIN ppp_gdp_per_capita
ON nominal_gdp_per_capita.country = ppp_gdp_per_capita.country

*/