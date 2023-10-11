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
                    WHEN totalNetWasteTrade > 0 AND netWasteTradeMT < 0 THEN netWasteTradeMT - totalNetWasteTrade * netWasteTradeMT / netWasteTradeMTPos
                    WHEN totalNetWasteTrade < 0 AND netWasteTradeMT > 0 THEN netWasteTradeMT - totalNetWasteTrade * netWasteTradeMT / netWasteTradeMTNeg
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
                    ) AS netWasteTradeMTPos,
                    sum(
                        CASE
                            WHEN netWasteTradeMT < 0 THEN netWasteTradeMT
                            ELSE 0
                        END
                    ) AS netWasteTradeMTNeg,
                    sum(netWasteTradeMT) AS totalNetWasteTrade
                FROM
                    {table_name}
                GROUP BY
                    year
            ) totals
        ON
            {table_name}.year = totals.year
        WHERE
            {table_name}.year > 2020 OR {table_name}.year < 2007
    ) updated
WHERE
    updated.year = {table_name}.year
    AND updated.region = {table_name}.region