INSERT INTO consumption_secondary_pending (year, region, majorMarketSector, consumptionMT)
SELECT
    consumption_secondary.year AS year,
    consumption_secondary.region AS region,
    consumption_secondary.majorMarketSector AS majorMarketSector,
    consumption_secondary.consumptionMT AS consumptionMT
FROM
    consumption_secondary
WHERE
    consumption_secondary.consumptionMT IS NOT NULL
