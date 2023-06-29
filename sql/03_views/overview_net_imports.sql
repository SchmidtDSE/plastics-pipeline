CREATE VIEW overview_net_imports AS
SELECT
    year,
    region,
    SUM(
        CASE
            WHEN type = 'articles' THEN netMT
            ELSE 0
        END
    ) AS articlesNetMT,
    SUM(
        CASE
            WHEN type = 'fibers' THEN netMT
            ELSE 0
        END
    ) AS fibersNetMT,
    SUM(
        CASE
            WHEN type = 'goods' THEN netMT
            ELSE 0
        END
    ) AS goodsNetMT,
    SUM(
        CASE
            WHEN type = 'resin' THEN netMT
            ELSE 0
        END
    ) AS resinNetMT
FROM
    net_imports
GROUP BY
    year,
    region
