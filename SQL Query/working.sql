SELECT 
    Defect_Date, 
    DC, 
    SKU, 
    partition_rank, 
    SUM(TOTAL) AS TOTAL, 
    SUM(REJECTS) AS REJECTS
FROM (
    SELECT 
        BUSINESS_DATE AS Defect_Date,
        DC,
        SKU,
        FLOOR(SAFE_DIVIDE(RANK() OVER (
            PARTITION BY BUSINESS_DATE, SKU 
            ORDER BY LOCAL_TS ASC
        ), 1000)) + 1 AS partition_rank,
        COUNT(*) AS TOTAL, 
        COUNTIF(ERROR_CODE != 'Pass') AS REJECTS
    FROM `wmt-edw-sandbox.SYMBOTIC_DATA.snowflake_cis_actions`
    WHERE EXTRACT(YEAR FROM BUSINESS_DATE) >= 2025
    GROUP BY BUSINESS_DATE, DC, SKU, partition_rank
)
GROUP BY Defect_Date, DC, SKU, partition_rank
ORDER BY Defect_Date DESC, SKU DESC
LIMIT 1000
