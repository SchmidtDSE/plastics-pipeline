CREATE VIEW net_waste_trade AS
SELECT
    year AS year,
    region AS region,
    netTons / 1000000.0 AS netMT
FROM
    raw_waste_trade
