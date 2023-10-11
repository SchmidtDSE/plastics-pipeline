CREATE VIEW instance_waste_trade_normal AS
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