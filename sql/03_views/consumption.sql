CREATE VIEW consumption AS
SELECT
    with_sector.year AS year,
    with_sector.region AS region,
    with_sector.majorMarketSector AS majorMarketSector,
    with_sector.consumptionMTNoTrade + overview_sector_trade.netMT AS consumptionMT
FROM
    (
        SELECT
            with_metrics.year AS year,
            with_metrics.region AS region,
            overview_end_use.majorMarketSector AS majorMarketSector,
            with_metrics.converterConsumptionMT * overview_end_use.percent AS consumptionMTNoTrade,
            with_metrics.importGoodsMT AS importGoodsMT
        FROM
            (
                SELECT
                    year AS year,
                    region AS region,
                    produceResinMT+importResinMT+additivesMT AS converterConsumptionMT,
                    importGoodsMT AS importGoodsMT
                FROM
                    overview_inputs
                GROUP BY
                    year,
                    region
            ) with_metrics
        LEFT OUTER JOIN
            overview_end_use
        ON
            overview_end_use.region = with_metrics.region
    ) with_sector
LEFT OUTER JOIN
    overview_sector_trade
ON
    overview_sector_trade.type = with_sector.majorMarketSector
    AND overview_sector_trade.year = with_sector.year
    AND overview_sector_trade.region = with_sector.region
UNION ALL
SELECT
    with_metrics.year AS year,
    with_metrics.region AS region,
    'Textile' AS majorMarketSector,
    with_metrics.textileMT + overview_sector_trade.netMT AS consumptionMT
FROM
    (
        SELECT
            year AS year,
            region AS region,
            produceFiberMT AS textileMT
        FROM
            overview_inputs
        GROUP BY
            year,
            region
    ) with_metrics
LEFT OUTER JOIN
    overview_sector_trade
ON
    overview_sector_trade.type = 'Textile'
    AND overview_sector_trade.year = with_metrics.year
    AND overview_sector_trade.region = with_metrics.region