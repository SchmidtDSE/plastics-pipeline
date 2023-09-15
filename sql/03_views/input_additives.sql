CREATE VIEW input_additives AS
SELECT
    year AS year,
    'china' AS region,
    china AS amountMT
FROM
    raw_additives
UNION ALL
SELECT
    year AS year,
    'nafta' AS region,
    nafta AS amountMT
FROM
    raw_additives
UNION ALL
SELECT
    year AS year,
    'eu30' AS region,
    eu30 AS amountMT
FROM
    raw_additives
UNION ALL
SELECT
    year AS year,
    'row' AS region,
    row AS amountMT
FROM
    raw_additives