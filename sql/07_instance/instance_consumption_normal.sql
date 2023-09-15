CREATE VIEW instance_consumption_normal AS
SELECT
    auxiliary.region AS region,
    auxiliary.year AS year,
    auxiliary.population AS population,
    auxiliary.gdp AS gdp,
    consumption.majorMarketSector AS majorMarketSector,
    consumption.consumptionMT AS consumptionMT
FROM
    auxiliary
INNER JOIN
    consumption
ON
    auxiliary.year = consumption.year
    AND auxiliary.region = consumption.region