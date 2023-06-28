CREATE VIEW overview_inputs AS
SELECT
    year,
    region,
    SUM(
        CASE
            WHEN source = 'production' AND type = 'fiber' THEN amountMT
            ELSE 0
        END
    ) AS produceFiberMT,
    SUM(
        CASE
            WHEN source = 'production' AND type = 'resin' THEN amountMT
            ELSE 0
        END
    ) AS produceResinMT,
    SUM(
        CASE
            WHEN source = 'trade' AND type = 'resin' THEN amountMT
            ELSE 0
        END
    ) AS importResinMT,
    SUM(
        CASE
            WHEN source = 'trade' AND type = 'articles' THEN amountMT
            ELSE 0
        END
    ) AS importArticlesMT,
    SUM(
        CASE
            WHEN source = 'trade' AND type = 'goods' THEN amountMT
            ELSE 0
        END
    ) AS importGoodsMT,
    SUM(
        CASE
            WHEN source = 'trade' AND type = 'fiber' THEN amountMT
            ELSE 0
        END
    ) AS importFiberMT,
    SUM(
        CASE
            WHEN source = 'additive' AND type = 'additive' THEN amountMT
            ELSE 0
        END
    ) AS additivesMT
FROM
    inputs
GROUP BY
    year,
    region
ORDER BY
    year DESC,
    region ASC