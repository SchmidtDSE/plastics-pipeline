CREATE VIEW net_imports AS
SELECT
    year,
    'china' AS region,
    'articles' AS type,
    china AS netMT
FROM
    raw_net_import_articles
UNION ALL
SELECT
    year,
    'nafta' AS region,
    'articles' AS type,
    nafta AS netMT
FROM
    raw_net_import_articles
UNION ALL
SELECT
    year,
    'eu30' AS region,
    'articles' AS type,
    eu30 AS netMT
FROM
    raw_net_import_articles
UNION ALL
SELECT
    year,
    'row' AS region,
    'articles' AS type,
    row AS netMT
FROM
    raw_net_import_articles
UNION ALL
SELECT
    year,
    'china' AS region,
    'fibers' AS type,
    china AS netMT
FROM
    raw_net_import_fibers
UNION ALL
SELECT
    year,
    'nafta' AS region,
    'fibers' AS type,
    nafta AS netMT
FROM
    raw_net_import_fibers
UNION ALL
SELECT
    year,
    'eu30' AS region,
    'fibers' AS type,
    eu30 AS netMT
FROM
    raw_net_import_fibers
UNION ALL
SELECT
    year,
    'row' AS region,
    'fibers' AS type,
    row AS netMT
FROM
    raw_net_import_fibers
UNION ALL
SELECT
    year,
    'china' AS region,
    'goods' AS type,
    china AS netMT
FROM
    raw_net_import_finished_goods
UNION ALL
SELECT
    year,
    'nafta' AS region,
    'goods' AS type,
    nafta AS netMT
FROM
    raw_net_import_finished_goods
UNION ALL
SELECT
    year,
    'eu30' AS region,
    'goods' AS type,
    eu30 AS netMT
FROM
    raw_net_import_finished_goods
UNION ALL
SELECT
    year,
    'row' AS region,
    'goods' AS type,
    row AS netMT
FROM
    raw_net_import_finished_goods
UNION ALL
SELECT
    year,
    'china' AS region,
    'resin' AS type,
    china AS netMT
FROM
    raw_net_import_resin
UNION ALL
SELECT
    year,
    'nafta' AS region,
    'resin' AS type,
    nafta AS netMT
FROM
    raw_net_import_resin
UNION ALL
SELECT
    year,
    'eu30' AS region,
    'resin' AS type,
    eu30 AS netMT
FROM
    raw_net_import_resin
UNION ALL
SELECT
    year,
    'row' AS region,
    'resin' AS type,
    row AS netMT
FROM
    raw_net_import_resin
