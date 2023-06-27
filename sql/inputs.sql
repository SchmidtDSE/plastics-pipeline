CREATE VIEW inputs AS
SELECT
    year AS year,
    region AS region,
    'additive' AS source,
    'additive' AS type,
    amountMT AS amountMT
FROM
    input_additives
UNION ALL
SELECT
    year AS year,
    region AS region,
    'trade' AS source,
    type AS type,
    amountMT AS amountMT
FROM
    input_import
UNION ALL
SELECT
    year AS year,
    region AS region,
    'production' AS source,
    type AS type,
    amountMT AS amountMT
FROM
    input_production