CREATE VIEW consumption_secondary AS
SELECT
    year AS year,
    region AS region,
    'Agriculture' AS majorMarketSector,
    consumptionAgricultureMT AS consumptionMT
FROM
    consumption_secondary
UNION ALL
SELECT
    year AS year,
    region AS region,
    'Building_Construction' AS majorMarketSector,
    consumptionConstructionMT AS consumptionMT
FROM
    consumption_secondary
UNION ALL
SELECT
    year AS year,
    region AS region,
    'Electrical_Electronic' AS majorMarketSector,
    consumptionElectronicMT AS consumptionMT
FROM
    consumption_secondary
UNION ALL
SELECT
    year AS year,
    region AS region,
    'Household_Leisure_Sports' AS majorMarketSector,
    consumptionHouseholdLeisureSportsMT AS consumptionMT
FROM
    consumption_secondary
UNION ALL
SELECT
    year AS year,
    region AS region,
    'Packaging' AS majorMarketSector,
    consumptionPackagingMT AS consumptionMT
FROM
    consumption_secondary
UNION ALL
SELECT
    year AS year,
    region AS region,
    'Transportation' AS majorMarketSector,
    consumptionTransportationMT AS consumptionMT
FROM
    consumption_secondary
UNION ALL
SELECT
    year AS year,
    region AS region,
    'Textile' AS majorMarketSector,
    consumptionTextileMT AS consumptionMT
FROM
    consumption_secondary
UNION ALL
SELECT
    year AS year,
    region AS region,
    'Others' AS majorMarketSector,
    consumptionOtherMT AS consumptionMT
FROM
    consumption_secondary
