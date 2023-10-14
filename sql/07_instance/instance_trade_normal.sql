CREATE VIEW instance_trade_normal AS
SELECT
    without_consumption.year AS year,
    without_consumption.region AS region,
    without_consumption.type AS type,
    without_consumption.netMT AS netMT,
    without_consumption.population AS population,
    without_consumption.gdp AS gdp,
    consumption_summary.totalConsumption AS totalConsumption
FROM
    (
        SELECT
            net_imports.year AS year,
            net_imports.region AS region,
            net_imports.type AS type,
            net_imports.netMT AS netMT,
            auxiliary.population AS population,
            auxiliary.gdp AS gdp
        FROM
            net_imports
        INNER JOIN
            auxiliary
        ON
            net_imports.year = auxiliary.year
            AND net_imports.region = auxiliary.region
    ) without_consumption
INNER JOIN
    (
        SELECT
            year,
            region,
            sum(consumptionMT) AS totalConsumption
        FROM
            consumption
        GROUP BY
            year,
            region
    ) consumption_summary
ON
    without_consumption.year = consumption_summary.year
    AND without_consumption.region = consumption_summary.region