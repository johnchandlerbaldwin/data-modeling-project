{{ config(materialized='view')}}

WITH hdi_stg1 AS (

        SELECT ANation AS country,
        ARowNumber AS RowNumber,
        AHDI AS HDI,
        COUNT(AHDI) OVER (ORDER BY ARowNumber ASC) AS grp
        FROM economic_indicators.hdi
        WHERE SAFE_CAST(AHDI AS NUMERIC) IS NOT NULL),

    hdi_stg2 AS (
    SELECT RowNumber, country, MAX(HDI) OVER (PARTITION BY grp ORDER BY RowNumber ASC) AS hdi
    FROM hdi_stg1)

SELECT country, hdi FROM hdi_stg2