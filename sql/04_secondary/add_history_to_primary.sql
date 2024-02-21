CREATE VIEW consumption_primary_restructure AS
SELECT
    year,
    region,
    consumptionAgricultureMT,
    consumptionConstructionMT,
    consumptionElectronicMT,
    consumptionHouseholdLeisureSportsMT,
    consumptionPackagingMT,
    consumptionTransportationMT,
    consumptionTextileMT,
    consumptionOtherMT,
    eolRecyclingPercent,
    eolIncinerationPercent,
    eolLandfillPercent,
    eolMismanagedPercent,
    netImportArticlesMT,
    netImportFibersMT,
    netImportGoodsMT,
    netImportResinMT,
    netWasteTradeMT
FROM
    consumption_primary_restructure_no_historic
UNION ALL
SELECT
    raw_future.year AS year,
    raw_future.region AS region,
    raw_future.consumptionAgricultureMT AS consumptionAgricultureMT,
    raw_future.consumptionConstructionMT AS consumptionConstructionMT,
    raw_future.consumptionElectronicMT AS consumptionElectronicMT,
    raw_future.consumptionHouseholdLeisureSportsMT AS consumptionHouseholdLeisureSportsMT,
    raw_future.consumptionPackagingMT AS consumptionPackagingMT,
    raw_future.consumptionTransportationMT AS consumptionTransportationMT,
    raw_future.consumptionTextileMT AS consumptionTextileMT,
    raw_future.consumptionOtherMT AS consumptionOtherMT,
    historic_recycling_estimate.percent AS eolRecyclingPercent,
    (1 - historic_recycling_estimate.percent) / 3 AS eolIncinerationPercent,
    (1 - historic_recycling_estimate.percent) / 3 AS eolLandfillPercent,
    (1 - historic_recycling_estimate.percent) / 3 AS eolMismanagedPercent,
    0 AS netImportArticlesMT,
    0 AS netImportFibersMT,
    0 AS netImportGoodsMT,
    0 AS netImportResinMT,
    0 AS netWasteTradeMT
FROM
    raw_future
INNER JOIN
    historic_recycling_estimate
ON
    historic_recycling_estimate.year = raw_future.year
    AND historic_recycling_estimate.region = raw_future.region
WHERE
    raw_future.year < 2005