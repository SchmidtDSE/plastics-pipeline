UPDATE
    {table_name}
SET
    netWasteTradeMT = updated.netWasteTradeMT
FROM
    (
        SELECT
            {table_name}.year AS year,
            {table_name}.region AS region,
            (
                CASE
                    WHEN totalNetWasteTradeMT > 0 AND netWasteTradeMT > 0 THEN netWasteTradeMT - totalNetWasteTradeMT * netWasteTradeMT / totalWasteTradeMTPos
                    WHEN totalNetWasteTradeMT < 0 AND netWasteTradeMT < 0 THEN netWasteTradeMT - totalNetWasteTradeMT * netWasteTradeMT / totalWasteTradeMTNeg
                    ELSE netWasteTradeMT
                END
            ) AS netWasteTradeMT
        FROM
            {table_name}
        INNER JOIN
            (
                SELECT
                    year,
                    sum(
                        CASE
                            WHEN netWasteTradeMT > 0 THEN netWasteTradeMT
                            ELSE 0
                        END 
                    ) AS totalWasteTradeMTPos,
                    sum(
                        CASE
                            WHEN netWasteTradeMT < 0 THEN netWasteTradeMT
                            ELSE 0
                        END
                    ) AS totalWasteTradeMTNeg,
                    sum(netWasteTradeMT) AS totalNetWasteTradeMT
                FROM
                    {table_name}
                GROUP BY
                    year
            ) totals
        ON
            {table_name}.year = totals.year
    ) updated
WHERE
    updated.year = {table_name}.year
    AND updated.region = {table_name}.region