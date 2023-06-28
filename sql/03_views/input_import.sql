CREATE VIEW input_import AS
SELECT
    year AS year,
    'china' AS region,
    'articles' AS type,
    china AS amountMT
FROM
    raw_net_import_articles
UNION ALL
SELECT
    year AS year,
    'nafta' AS region,
    'articles' AS type,
    nafta AS amountMT
FROM
    raw_net_import_articles
UNION ALL
SELECT
    year AS year,
    'eu30' AS region,
    'articles' AS type,
    eu30 AS amountMT
FROM
    raw_net_import_articles
UNION ALL
SELECT
    year AS year,
    'row' AS region,
    'articles' AS type,
    row AS amountMT
FROM
    raw_net_import_articles
UNION ALL
SELECT
    year AS year,
    'china' AS region,
    'fiber' AS type,
    china AS amountMT
FROM
    raw_net_import_fibers
UNION ALL
SELECT
    year AS year,
    'nafta' AS region,
    'fiber' AS type,
    nafta AS amountMT
FROM
    raw_net_import_fibers
UNION ALL
SELECT
    year AS year,
    'eu30' AS region,
    'fiber' AS type,
    eu30 AS amountMT
FROM
    raw_net_import_fibers
UNION ALL
SELECT
    year AS year,
    'row' AS region,
    'fiber' AS type,
    row AS amountMT
FROM
    raw_net_import_fibers
UNION ALL
SELECT
    year AS year,
    'china' AS region,
    'goods' AS type,
    china AS amountMT
FROM
    raw_net_import_finished_goods
UNION ALL
SELECT
    year AS year,
    'nafta' AS region,
    'goods' AS type,
    nafta AS amountMT
FROM
    raw_net_import_finished_goods
UNION ALL
SELECT
    year AS year,
    'eu30' AS region,
    'goods' AS type,
    eu30 AS amountMT
FROM
    raw_net_import_finished_goods
UNION ALL
SELECT
    year AS year,
    'row' AS region,
    'goods' AS type,
    row AS amountMT
FROM
    raw_net_import_finished_goods
UNION ALL
SELECT
    year AS year,
    'china' AS region,
    'resin' AS type,
    china AS amountMT
FROM
    raw_net_import_resin
UNION ALL
SELECT
    year AS year,
    'nafta' AS region,
    'resin' AS type,
    nafta AS amountMT
FROM
    raw_net_import_resin
UNION ALL
SELECT
    year AS year,
    'eu30' AS region,
    'resin' AS type,
    eu30 AS amountMT
FROM
    raw_net_import_resin
UNION ALL
SELECT
    year AS year,
    'row' AS region,
    'resin' AS type,
    row AS amountMT
FROM
    raw_net_import_resin