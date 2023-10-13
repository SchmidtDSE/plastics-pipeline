CREATE VIEW net_waste_trade AS
SELECT
    year AS year,
    region AS region,
    netMT AS netMT
FROM
    raw_waste_trade
