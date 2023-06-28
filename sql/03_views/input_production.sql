CREATE VIEW input_production AS
SELECT
    year AS year,
    'china' AS region,
    'fiber' AS type,
    china AS amountMT
FROM
    raw_production_fiber
UNION ALL
SELECT
    year AS year,
    'nafta' AS region,
    'fiber' AS type,
    nafta AS amountMT
FROM
    raw_production_fiber
UNION ALL
SELECT
    year AS year,
    'eu30' AS region,
    'fiber' AS type,
    eu30 AS amountMT
FROM
    raw_production_fiber
UNION ALL
SELECT
    year AS year,
    'row' AS region,
    'fiber' AS type,
    row AS amountMT
FROM
    raw_production_fiber
UNION ALL
SELECT
    year AS year,
    'china' AS region,
    'resin' AS type,
    china AS amountMT
FROM
    raw_production_resin
UNION ALL
SELECT
    year AS year,
    'nafta' AS region,
    'resin' AS type,
    nafta AS amountMT
FROM
    raw_production_resin
UNION ALL
SELECT
    year AS year,
    'eu30' AS region,
    'resin' AS type,
    eu30 AS amountMT
FROM
    raw_production_resin
UNION ALL
SELECT
    year AS year,
    'row' AS region,
    'resin' AS type,
    row AS amountMT
FROM
    raw_production_resin