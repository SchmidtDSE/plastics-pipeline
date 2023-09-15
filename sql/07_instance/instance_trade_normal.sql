CREATE VIEW instance_trade_normal AS
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