CREATE VIEW consumption AS
SELECT
    consumption_primary.year AS year,
    consumption_primary.region AS region,
    consumption_primary.majorMarketSector AS majorMarketSector,
    consumption_primary.consumptionMT AS primaryConsumptionMT,
    consumption_secondary.consumptionMT AS secondaryConsumptionMT,
    (
        consumption_primary.consumptionMT +
        consumption_secondary.consumptionMT
    ) AS consumptionMT
FROM
    consumption_primary
LEFT JOIN
    consumption_secondary
ON
    consumption_primary.year = consumption_secondary.year
    AND consumption_primary.region = consumption_secondary.region
    AND consumption_primary.majorMarketSector = consumption_secondary.majorMarketSector
