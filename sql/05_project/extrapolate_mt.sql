CREATE VIEW extrapolate_mt AS
SELECT
    region AS region,
    year AS year,
    inputProduceFiberMT AS inputProduceFiberMT,
    inputProduceResinMT AS inputProduceResinMT,
    inputImportResinMT AS inputImportResinMT,
    inputImportArticlesMT AS inputImportArticlesMT,
    inputImportGoodsMT AS inputImportGoodsMT,
    inputImportFiberMT AS inputImportFiberMT,
    inputAdditivesMT AS inputAdditivesMT,
    consumptionAgriculturePercent * inputTotal AS consumptionAgricultureMT,
    consumptionConstructionPercent * inputTotal AS consumptionConstructionMT,
    consumptionElectronicPercent * inputTotal AS consumptionElectronicMT,
    consumptionHouseholdLeisureSportsPercent * inputTotal AS consumptionHouseholdLeisureSportsMT,
    consumptionPackagingPercent * inputTotal AS consumptionPackagingMT,
    consumptionTransporationPercent * inputTotal AS consumptionTransporationMT,
    consumptionTextitlePercent * inputTotal AS consumptionTextitleMT,
    consumptionOtherPercent * inputTotal AS consumptionOtherMT,
    eolRecyclingPercent * inputTotal AS eolRecyclingMT,
    eolIncinerationPercent * inputTotal AS eolIncinerationMT,
    eolLandfillPercent * inputTotal AS eolLandfillMT,
    eolMismanagedPercent * inputTotal AS eolMismanagedMT
FROM
    (
        SELECT
            region AS region,
            year AS year,
            inputProduceFiberMT AS inputProduceFiberMT,
            inputProduceResinMT AS inputProduceResinMT,
            inputImportResinMT AS inputImportResinMT,
            inputImportArticlesMT AS inputImportArticlesMT,
            inputImportGoodsMT AS inputImportGoodsMT,
            inputImportFiberMT AS inputImportFiberMT,
            inputAdditivesMT AS inputAdditivesMT,
            (
                inputProduceFiberMT + 
                inputProduceResinMT + 
                inputImportResinMT + 
                inputImportArticlesMT + 
                inputImportGoodsMT + 
                inputImportFiberMT + 
                inputAdditivesMT
            ) AS inputTotal,
            consumptionAgriculturePercent AS consumptionAgriculturePercent,
            consumptionConstructionPercent AS consumptionConstructionPercent,
            consumptionElectronicPercent AS consumptionElectronicPercent,
            consumptionHouseholdLeisureSportsPercent AS consumptionHouseholdLeisureSportsPercent,
            consumptionPackagingPercent AS consumptionPackagingPercent,
            consumptionTransporationPercent AS consumptionTransporationPercent,
            consumptionTextitlePercent AS consumptionTextitlePercent,
            consumptionOtherPercent AS consumptionOtherPercent,
            eolRecyclingPercent AS eolRecyclingPercent,
            eolIncinerationPercent AS eolIncinerationPercent,
            eolLandfillPercent AS eolLandfillPercent,
            eolMismanagedPercent AS eolMismanagedPercent
        FROM
            extrapolate_percents
    ) with_total
