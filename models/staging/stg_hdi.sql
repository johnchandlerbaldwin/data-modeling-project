{{ config(materialized='view')}}

WITH hdi_stg1 AS (

        SELECT ANation AS country,
        ARowNumber AS RowNumber,
        AHDI AS HDI,
        sum(case when AHDI is not null then 1 else 0 end) OVER (ORDER BY ARowNumber ASC) AS grp
        FROM {{ source('google-sheets', 'hdi') }} 
        /*WHERE SAFE_CAST(AHDI AS NUMERIC) IS NOT NULL*/),

    hdi_stg2 AS (
    SELECT RowNumber, country, MAX(HDI) OVER (PARTITION BY grp ORDER BY RowNumber ASC) AS hdi
    FROM hdi_stg1),

    hdi_stg3 AS (
        SELECT RowNumber, 
        CASE WHEN country = 'Republic of the Congo' THEN 'Congo' ELSE country END AS country,
        hdi 
        FROM hdi_stg2
    )

SELECT * FROM hdi_stg3
WHERE country IS NOT NULL
ORDER BY country ASC