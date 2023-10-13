CREATE VIEW net_waste_trade AS
SELECT
    year AS year,
    region AS region,
    netMT * -1 AS netMT
FROM
    raw_waste_trade
