UPDATE
    {table_name}
SET
    netWasteTradeMT = updated.netWasteTradeMT
FROM
    (
        SELECT
            {table_name}.year AS year,
            {table_name}.region AS region,
            netWasteTradeMT - totalNetWasteTradeAvg AS netWasteTradeMT
        FROM
            {table_name}
        INNER JOIN
            (
                SELECT
                    year,
                    avg(netWasteTradeMT) AS totalNetWasteTradeAvg
                FROM
                    {table_name}
                WHERE
                    region != 'china'
                    OR netWasteTradeMT > 0
                GROUP BY
                    year
            ) totals
        ON
            {table_name}.year = totals.year
        WHERE
            {table_name}.year > 2020 OR {table_name}.year < 2007
            AND (region != 'china' OR netWasteTradeMT > 0)
    ) updated
WHERE
    updated.year = {table_name}.year
    AND updated.region = {table_name}.region