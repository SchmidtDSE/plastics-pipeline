CREATE VIEW overview_end_use AS
SELECT
    majorMarketSector AS majorMarketSector,
    'china' AS region,
    total AS percent
FROM
    raw_end_use_china
UNION ALL
SELECT
    majorMarketSector AS majorMarketSector,
    'eu30' AS region,
    total AS percent
FROM
    raw_end_use_eu30
UNION ALL
SELECT
    majorMarketSector AS majorMarketSector,
    'nafta' AS region,
    total AS percent
FROM
    raw_end_use_nafta
UNION ALL
SELECT
    majorMarketSector AS majorMarketSector,
    'row' AS region,
    total AS percent
FROM
    raw_end_use_row