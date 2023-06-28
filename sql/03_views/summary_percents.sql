CREATE VIEW summary_percents AS
SELECT
    with_total.year AS year,
    with_total.region AS region,
    with_total.inputProduceFiberMT AS inputProduceFiberMT,
    with_total.inputProduceResinMT AS inputProduceResinMT,
    with_total.inputImportResinMT AS inputImportResinMT,
    with_total.inputImportArticlesMT AS inputImportArticlesMT,
    with_total.inputImportGoodsMT AS inputImportGoodsMT,
    with_total.inputImportFiberMT AS inputImportFiberMT,
    with_total.inputAdditivesMT AS inputAdditivesMT,
    with_total.consumptionAgricultureMT / with_total.totalConsumption AS consumptionAgriculturePercent,
    with_total.consumptionConstructionMT / with_total.totalConsumption AS consumptionConstructionPercent,
    with_total.consumptionElectronicMT / with_total.totalConsumption AS consumptionElectronicPercent,
    with_total.consumptionHouseholdLeisureSportsMT / with_total.totalConsumption AS consumptionHouseholdLeisureSportsPercent,
    with_total.consumptionPackagingMT / with_total.totalConsumption AS consumptionPackagingPercent,
    with_total.consumptionTransporationMT / with_total.totalConsumption AS consumptionTransporationPercent,
    with_total.consumptionTextitleMT / with_total.totalConsumption AS consumptionTextitlePercent,
    with_total.consumptionOtherMT / with_total.totalConsumption AS consumptionOtherPercent,
    with_total.eolRecyclingPercent AS eolRecyclingPercent,
    with_total.eolIncinerationPercent AS eolIncinerationPercent,
    with_total.eolLandfillPercent AS eolLandfillPercent,
    with_total.eolMismanagedPercent AS eolMismanagedPercent
FROM
    (
        SELECT
            summary.year AS year,
            summary.region AS region,
            summary.inputProduceFiberMT AS inputProduceFiberMT,
            summary.inputProduceResinMT AS inputProduceResinMT,
            summary.inputImportResinMT AS inputImportResinMT,
            summary.inputImportArticlesMT AS inputImportArticlesMT,
            summary.inputImportGoodsMT AS inputImportGoodsMT,
            summary.inputImportFiberMT AS inputImportFiberMT,
            summary.inputAdditivesMT AS inputAdditivesMT,
            summary.consumptionAgricultureMT AS consumptionAgricultureMT,
            summary.consumptionConstructionMT AS consumptionConstructionMT,
            summary.consumptionElectronicMT AS consumptionElectronicMT,
            summary.consumptionHouseholdLeisureSportsMT AS consumptionHouseholdLeisureSportsMT,
            summary.consumptionPackagingMT AS consumptionPackagingMT,
            summary.consumptionTransporationMT AS consumptionTransporationMT,
            summary.consumptionTextitleMT AS consumptionTextitleMT,
            summary.consumptionOtherMT AS consumptionOtherMT,
            (consumptionAgricultureMT + consumptionConstructionMT + consumptionElectronicMT + consumptionHouseholdLeisureSportsMT + consumptionPackagingMT + consumptionTransporationMT + consumptionTextitleMT + consumptionOtherMT) AS totalConsumption,
            summary.eolRecyclingPercent AS eolRecyclingPercent,
            summary.eolIncinerationPercent AS eolIncinerationPercent,
            summary.eolLandfillPercent AS eolLandfillPercent,
            summary.eolMismanagedPercent AS eolMismanagedPercent
        FROM
            summary
    ) with_total