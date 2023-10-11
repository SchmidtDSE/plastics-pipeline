UPDATE
    {table_name}
SET
    netImportArticlesMT = updated.netImportArticlesMT,
    netImportFibersMT = updated.netImportFibersMT,
    netImportGoodsMT = updated.netImportGoodsMT,
    netImportResinMT = updated.netImportResinMT
FROM
    (
        SELECT
            {table_name}.year AS year,
            {table_name}.region AS region,
            netImportArticlesMT - netImportArticlesMTAvg AS netImportArticlesMT,
            netImportFibersMT - netImportFibersMTAvg AS netImportFibersMT,
            netImportGoodsMT - netImportGoodsMTAvg AS netImportGoodsMT,
            netImportResinMT - netImportResinMTAvg AS netImportResinMT
        FROM
            {table_name}
        INNER JOIN
            (
                SELECT
                    year,
                    avg(netImportArticlesMT) AS netImportArticlesMTAvg,
                    avg(netImportFibersMT) AS netImportFibersMTAvg,
                    avg(netImportGoodsMT) AS netImportGoodsMTAvg,
                    avg(netImportResinMT) AS netImportResinMTAvg
                FROM
                    {table_name}
                GROUP BY
                    year
            ) totals
        ON
            {table_name}.year = totals.year
        WHERE
            {table_name}.year > 2020 OR {table_name}.year < 2005
    ) updated
WHERE
    updated.year = {table_name}.year
    AND updated.region = {table_name}.region