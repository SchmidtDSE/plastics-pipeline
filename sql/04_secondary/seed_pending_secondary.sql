CREATE TABLE consumption_secondary_pending AS
SELECT
    year,
    region,
    majorMarketSector,
    0 AS consumptionMT
FROM
    consumption_primary