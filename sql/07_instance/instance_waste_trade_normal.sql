CREATE VIEW instance_waste_trade_normal AS
SELECT
    without_consumption.year AS year,
    without_consumption.region AS region,
    without_consumption.netMT AS netMT,
    without_consumption.population AS population,
    without_consumption.gdp AS gdp,
    consumption_summary.totalConsumption AS totalConsumption
FROM
    (
        SELECT
            net_waste_trade.year AS year,
            net_waste_trade.region AS region,
            net_waste_trade.netMT AS netMT,
            auxiliary.population AS population,
            auxiliary.gdp AS gdp
        FROM
            net_waste_trade
        INNER JOIN
            auxiliary
        ON
            net_waste_trade.year = auxiliary.year
            AND net_waste_trade.region = auxiliary.region
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