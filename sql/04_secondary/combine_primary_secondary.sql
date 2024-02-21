CREATE VIEW consumption AS
SELECT
    consumption_primary.year AS year,
    consumption_primary.region AS region,
    consumption_primary.majorMarketSector AS majorMarketSector,
    consumption_primary.consumptionMT AS primaryConsumptionMT,
    pending_sum.consumptionMT AS secondaryConsumptionMT,
    (
        consumption_primary.consumptionMT +
        pending_sum.consumptionMT
    ) AS consumptionMT
FROM
    consumption_primary
LEFT JOIN
    (
        SELECT
            year,
            region,
            majorMarketSector,
            sum(
                CASE
                    WHEN consumptionMT < 0 THEN 0
                    ELSE consumptionMT
                END
            ) AS consumptionMT
        FROM
            consumption_secondary_pending
        GROUP BY
            year,
            region,
            majorMarketSector
    ) pending_sum
ON
    consumption_primary.year = pending_sum.year
    AND consumption_primary.region = pending_sum.region
    AND consumption_primary.majorMarketSector = pending_sum.majorMarketSector
