{{ config(materialized='view')}}

WITH hdi_stg1 AS (

    SELECT COALESCE(AUnnamed1, '0') AS change_since_2015,
    ANation AS country,
    AHDI AS HDI
    FROM economic_indicators.hdi
)

SELECT * FROM hdi_stg1