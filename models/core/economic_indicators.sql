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

),

countries_by_region AS (

    SELECT *
    FROM {{ ref('countries_by_region') }}
)

SELECT stg_nominal_gdp_per_capita.country, stg_hdi.hdi, stg_nominal_gdp_per_capita.nominal_gdp_per_capita, 
stg_population.population, stg_ppp_gdp_per_capita.ppp_gdp_per_capita, `sub-region`
FROM stg_nominal_gdp_per_capita
LEFT JOIN stg_hdi
ON stg_nominal_gdp_per_capita.country = stg_hdi.country
LEFT JOIN stg_population
ON stg_nominal_gdp_per_capita.country = stg_population.country
LEFT JOIN stg_ppp_gdp_per_capita
ON stg_nominal_gdp_per_capita.country = stg_ppp_gdp_per_capita.country
INNER JOIN countries_by_region
ON stg_nominal_gdp_per_capita.country = countries_by_region.name
WHERE population > 100000
ORDER BY country