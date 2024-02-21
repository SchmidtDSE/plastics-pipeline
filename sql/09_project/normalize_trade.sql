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
            (
                CASE
                    WHEN totalArticlesMT > 0 AND netImportArticlesMT > 0 THEN netImportArticlesMT - totalArticlesMT * netImportArticlesMT / totalImportArticlesMTPos
                    WHEN totalArticlesMT < 0 AND netImportArticlesMT < 0 THEN netImportArticlesMT - totalArticlesMT * netImportArticlesMT / totalImportArticlesMTNeg
                    ELSE netImportArticlesMT
                END
            ) AS netImportArticlesMT,
            (
                CASE
                    WHEN totalFibersMT > 0 AND netImportFibersMT > 0 THEN netImportFibersMT - totalFibersMT * netImportFibersMT / totalImportFibersMTPos
                    WHEN totalFibersMT < 0 AND netImportFibersMT < 0 THEN netImportFibersMT - totalFibersMT * netImportFibersMT / totalImportFibersMTNeg
                    ELSE netImportFibersMT
                END
            ) AS netImportFibersMT,
            (
                CASE
                    WHEN totalGoodsMT > 0 AND netImportGoodsMT > 0 THEN netImportGoodsMT - totalGoodsMT * netImportGoodsMT / totalImportGoodsMTPos
                    WHEN totalGoodsMT < 0 AND netImportGoodsMT < 0 THEN netImportGoodsMT - totalGoodsMT * netImportGoodsMT / totalImportGoodsMTNeg
                    ELSE netImportGoodsMT
                END
            ) AS netImportGoodsMT,
            (
                CASE
                    WHEN totalResinMT > 0 AND netImportResinMT > 0 THEN netImportResinMT - totalResinMT * netImportResinMT / totalImportResinMTPos
                    WHEN totalResinMT < 0 AND netImportResinMT < 0 THEN netImportResinMT - totalResinMT * netImportResinMT / totalImportResinMTNeg
                    ELSE netImportResinMT
                END
            ) AS netImportResinMT
        FROM
            {table_name}
        INNER JOIN
            (
                SELECT
                    year,
                    sum(
                        CASE
                            WHEN netImportArticlesMT > 0 THEN netImportArticlesMT
                            ELSE 0
                        END 
                    ) AS totalImportArticlesMTPos,
                    sum(
                        CASE
                            WHEN netImportArticlesMT < 0 THEN netImportArticlesMT
                            ELSE 0
                        END
                    ) AS totalImportArticlesMTNeg,
                    sum(
                        CASE
                            WHEN netImportFibersMT > 0 THEN netImportFibersMT
                            ELSE 0
                        END 
                    ) AS totalImportFibersMTPos,
                    sum(
                        CASE
                            WHEN netImportFibersMT < 0 THEN netImportFibersMT
                            ELSE 0
                        END
                    ) AS totalImportFibersMTNeg,
                    sum(
                        CASE
                            WHEN netImportGoodsMT > 0 THEN netImportGoodsMT
                            ELSE 0
                        END 
                    ) AS totalImportGoodsMTPos,
                    sum(
                        CASE
                            WHEN netImportGoodsMT < 0 THEN netImportGoodsMT
                            ELSE 0
                        END
                    ) AS totalImportGoodsMTNeg,
                    sum(
                        CASE
                            WHEN netImportResinMT > 0 THEN netImportResinMT
                            ELSE 0
                        END 
                    ) AS totalImportResinMTPos,
                    sum(
                        CASE
                            WHEN netImportResinMT < 0 THEN netImportResinMT
                            ELSE 0
                        END
                    ) AS totalImportResinMTNeg,
                    sum(netImportArticlesMT) AS totalArticlesMT,
                    sum(netImportFibersMT) AS totalFibersMT,
                    sum(netImportGoodsMT) AS totalGoodsMT,
                    sum(netImportResinMT) AS totalResinMT
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