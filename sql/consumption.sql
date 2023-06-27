CREATE VIEW consumption AS
SELECT
    with_sector.year AS year,
    with_sector.region AS region,
    with_sector.majorMarketSector AS majorMarketSector,
    with_sector.consumptionMTNoTrade + with_sector.importGoodsMT * raw_meta_sectors_2.percentage AS consumptionMT
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
                    produceResinMT+importResinMT+additivesMT+importArticlesMT AS converterConsumptionMT,
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
    raw_meta_sectors_2
ON
    raw_meta_sectors_2.majorMarketSector = with_sector.majorMarketSector
UNION ALL
SELECT
    with_metrics.year AS year,
    with_metrics.region AS region,
    'Textile' AS majorMarketSector,
    with_metrics.textileMT AS consumptionMT
FROM
    (
        SELECT
            year AS year,
            region AS region,
            produceFiberMT + importFiberMT AS textileMT
        FROM
            overview_inputs
        GROUP BY
            year,
            region
    ) with_metrics