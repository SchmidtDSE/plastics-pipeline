INSERT INTO consumption_primary_restructure (
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
)
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
    (
        consumptionAgricultureMT +
        consumptionConstructionMT +
        consumptionElectronicMT +
        consumptionHouseholdLeisureSportsMT +
        consumptionPackagingMT +
        consumptionTransportationMT +
        consumptionTextileMT +
        consumptionOtherMT
    ) * netImportArticlesPercent AS netImportArticlesMT,
    (
        consumptionAgricultureMT +
        consumptionConstructionMT +
        consumptionElectronicMT +
        consumptionHouseholdLeisureSportsMT +
        consumptionPackagingMT +
        consumptionTransportationMT +
        consumptionTextileMT +
        consumptionOtherMT
    ) * netImportFibersPercent AS netImportFibersMT,
    (
        consumptionAgricultureMT +
        consumptionConstructionMT +
        consumptionElectronicMT +
        consumptionHouseholdLeisureSportsMT +
        consumptionPackagingMT +
        consumptionTransportationMT +
        consumptionTextileMT +
        consumptionOtherMT
    ) * netImportGoodsPercent AS netImportGoodsMT,
    (
        consumptionAgricultureMT +
        consumptionConstructionMT +
        consumptionElectronicMT +
        consumptionHouseholdLeisureSportsMT +
        consumptionPackagingMT +
        consumptionTransportationMT +
        consumptionTextileMT +
        consumptionOtherMT
    ) * netImportResinPercent AS netImportResinMT,
    (
        consumptionAgricultureMT +
        consumptionConstructionMT +
        consumptionElectronicMT +
        consumptionHouseholdLeisureSportsMT +
        consumptionPackagingMT +
        consumptionTransportationMT +
        consumptionTextileMT +
        consumptionOtherMT
    ) * netWasteTradePercent AS netWasteTradeMT
FROM
    (
        SELECT
            consumption_secondary_restructure.year AS year,
            consumption_secondary_restructure.region AS region,
            (
                CASE
                    WHEN consumption_secondary_restructure.consumptionAgricultureMT IS NULL THEN 0
                    ELSE consumption_secondary_restructure.consumptionAgricultureMT
                END
            ) AS consumptionAgricultureMT,
            (
                CASE
                    WHEN consumption_secondary_restructure.consumptionConstructionMT IS NULL THEN 0
                    ELSE consumption_secondary_restructure.consumptionConstructionMT
                END
            ) AS consumptionConstructionMT,
            (
                CASE
                    WHEN consumption_secondary_restructure.consumptionElectronicMT IS NULL THEN 0
                    ELSE consumption_secondary_restructure.consumptionElectronicMT
                END
            ) AS consumptionElectronicMT,
            (
                CASE
                    WHEN consumption_secondary_restructure.consumptionHouseholdLeisureSportsMT IS NULL THEN 0
                    ELSE consumption_secondary_restructure.consumptionHouseholdLeisureSportsMT
                END
            ) AS consumptionHouseholdLeisureSportsMT,
            (
                CASE
                    WHEN consumption_secondary_restructure.consumptionPackagingMT IS NULL THEN 0
                    ELSE consumption_secondary_restructure.consumptionPackagingMT
                END
            ) AS consumptionPackagingMT,
            (
                CASE
                    WHEN consumption_secondary_restructure.consumptionTransportationMT IS NULL THEN 0
                    ELSE consumption_secondary_restructure.consumptionTransportationMT
                END
            ) AS consumptionTransportationMT,
            (
                CASE
                    WHEN consumption_secondary_restructure.consumptionTextileMT IS NULL THEN 0
                    ELSE consumption_secondary_restructure.consumptionTextileMT
                END
            ) AS consumptionTextileMT,
            (
                CASE
                    WHEN consumption_secondary_restructure.consumptionOtherMT IS NULL THEN 0
                    ELSE consumption_secondary_restructure.consumptionOtherMT
                END
            ) AS consumptionOtherMT,
            (
                CASE
                    WHEN supplemental.eolRecyclingPercent IS NULL THEN 0
                    ELSE supplemental.eolRecyclingPercent
                END
            ) AS eolRecyclingPercent,
            (
                CASE
                    WHEN supplemental.eolIncinerationPercent IS NULL THEN 0
                    ELSE supplemental.eolIncinerationPercent
                END
            ) AS eolIncinerationPercent,
            (
                CASE
                    WHEN supplemental.eolLandfillPercent IS NULL THEN 0
                    ELSE supplemental.eolLandfillPercent
                END
            ) AS eolLandfillPercent,
            (
                CASE
                    WHEN supplemental.eolMismanagedPercent IS NULL THEN 0
                    ELSE supplemental.eolMismanagedPercent
                END
            ) AS eolMismanagedPercent,
            (
                CASE
                    WHEN supplemental.netImportArticlesPercent IS NULL THEN 0
                    ELSE supplemental.netImportArticlesPercent
                END
            ) AS netImportArticlesPercent,
            (
                CASE
                    WHEN supplemental.netImportFibersPercent IS NULL THEN 0
                    ELSE supplemental.netImportFibersPercent
                END
            ) AS netImportFibersPercent,
            (
                CASE
                    WHEN supplemental.netImportGoodsPercent IS NULL THEN 0
                    ELSE supplemental.netImportGoodsPercent
                END
            ) AS netImportGoodsPercent,
            (
                CASE
                    WHEN supplemental.netImportResinPercent IS NULL THEN 0
                    ELSE supplemental.netImportResinPercent
                END
            ) AS netImportResinPercent,
            (
                CASE
                    WHEN supplemental.netWasteTradePercent IS NULL THEN 0
                    ELSE supplemental.netWasteTradePercent
                END
            ) AS netWasteTradePercent
        FROM
            consumption_secondary_restructure
        LEFT OUTER JOIN
            (
                SELECT
                    year,
                    region,
                    eolRecyclingPercent,
                    eolIncinerationPercent,
                    eolLandfillPercent,
                    eolMismanagedPercent,
                    netImportArticlesMT / (
                        consumptionAgricultureMT +
                        consumptionConstructionMT +
                        consumptionElectronicMT +
                        consumptionHouseholdLeisureSportsMT +
                        consumptionPackagingMT +
                        consumptionTransportationMT +
                        consumptionTextileMT +
                        consumptionOtherMT
                    ) AS netImportArticlesPercent,
                    netImportFibersMT / (
                        consumptionAgricultureMT +
                        consumptionConstructionMT +
                        consumptionElectronicMT +
                        consumptionHouseholdLeisureSportsMT +
                        consumptionPackagingMT +
                        consumptionTransportationMT +
                        consumptionTextileMT +
                        consumptionOtherMT
                    ) AS netImportFibersPercent,
                    netImportGoodsMT / (
                        consumptionAgricultureMT +
                        consumptionConstructionMT +
                        consumptionElectronicMT +
                        consumptionHouseholdLeisureSportsMT +
                        consumptionPackagingMT +
                        consumptionTransportationMT +
                        consumptionTextileMT +
                        consumptionOtherMT
                    ) AS netImportGoodsPercent,
                    netImportResinMT / (
                        consumptionAgricultureMT +
                        consumptionConstructionMT +
                        consumptionElectronicMT +
                        consumptionHouseholdLeisureSportsMT +
                        consumptionPackagingMT +
                        consumptionTransportationMT +
                        consumptionTextileMT +
                        consumptionOtherMT
                    ) AS netImportResinPercent,
                    netWasteTradeMT / (
                        consumptionAgricultureMT +
                        consumptionConstructionMT +
                        consumptionElectronicMT +
                        consumptionHouseholdLeisureSportsMT +
                        consumptionPackagingMT +
                        consumptionTransportationMT +
                        consumptionTextileMT +
                        consumptionOtherMT
                    ) AS netWasteTradePercent
                FROM
                    consumption_primary_restructure_no_historic
                UNION ALL
                SELECT
                    raw_future.year AS year,
                    raw_future.region AS region,
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
            ) supplemental
        ON
            supplemental.region = consumption_secondary_restructure.region
            AND supplemental.year = consumption_secondary_restructure.year
    ) joined
WHERE
    year <= 2021