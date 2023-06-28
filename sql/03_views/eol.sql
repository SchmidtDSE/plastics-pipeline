CREATE VIEW eol AS
SELECT
    year,
    'china' AS region,
    'recycling' AS type,
    CAST(REPLACE(recycling, '%', '') AS REAL) / 100 AS percent
FROM
    raw_eol_china
UNION ALL
SELECT
    year,
    'china' AS region,
    'incineration' AS type,
    CAST(REPLACE(incineration, '%', '') AS REAL) / 100 AS percent
FROM
    raw_eol_china
UNION ALL
SELECT
    year,
    'china' AS region,
    'landfill' AS type,
    CAST(REPLACE(landfill, '%', '') AS REAL) / 100 AS percent
FROM
    raw_eol_china
UNION ALL
SELECT
    year,
    'china' AS region,
    'mismanaged' AS type,
    CAST(REPLACE(mismanaged, '%', '') AS REAL) / 100 AS percent
FROM
    raw_eol_china
UNION ALL
SELECT
    year,
    'eu30' AS region,
    'recycling' AS type,
    CAST(REPLACE(recycling, '%', '') AS REAL) / 100 AS percent
FROM
    raw_eol_eu30
UNION ALL
SELECT
    year,
    'eu30' AS region,
    'incineration' AS type,
    CAST(REPLACE(incineration, '%', '') AS REAL) / 100 AS percent
FROM
    raw_eol_eu30
UNION ALL
SELECT
    year,
    'eu30' AS region,
    'landfill' AS type,
    CAST(REPLACE(landfill, '%', '') AS REAL) / 100 AS percent
FROM
    raw_eol_eu30
UNION ALL
SELECT
    year,
    'eu30' AS region,
    'mismanaged' AS type,
    CAST(REPLACE(mismanaged, '%', '') AS REAL) / 100 AS percent
FROM
    raw_eol_eu30
UNION ALL
SELECT
    year,
    'nafta' AS region,
    'recycling' AS type,
    CAST(REPLACE(recycling, '%', '') AS REAL) / 100 AS percent
FROM
    raw_eol_nafta
UNION ALL
SELECT
    year,
    'nafta' AS region,
    'incineration' AS type,
    CAST(REPLACE(incineration, '%', '') AS REAL) / 100 AS percent
FROM
    raw_eol_nafta
UNION ALL
SELECT
    year,
    'nafta' AS region,
    'landfill' AS type,
    CAST(REPLACE(landfill, '%', '') AS REAL) / 100 AS percent
FROM
    raw_eol_nafta
UNION ALL
SELECT
    year,
    'nafta' AS region,
    'mismanaged' AS type,
    CAST(REPLACE(mismanaged, '%', '') AS REAL) / 100 AS percent
FROM
    raw_eol_nafta
UNION ALL
SELECT
    year,
    'row' AS region,
    'recycling' AS type,
    CAST(REPLACE(recycling, '%', '') AS REAL) / 100 AS percent
FROM
    raw_eol_row
UNION ALL
SELECT
    year,
    'row' AS region,
    'incineration' AS type,
    CAST(REPLACE(incineration, '%', '') AS REAL) / 100 AS percent
FROM
    raw_eol_row
UNION ALL
SELECT
    year,
    'row' AS region,
    'landfill' AS type,
    CAST(REPLACE(landfill, '%', '') AS REAL) / 100 AS percent
FROM
    raw_eol_row
UNION ALL
SELECT
    year,
    'row' AS region,
    'mismanaged' AS type,
    CAST(REPLACE(mismanaged, '%', '') AS REAL) / 100 AS percent
FROM
    raw_eol_row