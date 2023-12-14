{{ config(materialized='view')}}

WITH hdi_stg1 AS (

    SELECT ANation AS country,
    AHDI AS HDI
    FROM economic_indicators.hdi
)

SELECT * FROM hdi_stg1