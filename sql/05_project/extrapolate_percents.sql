CREATE VIEW extrapolate_percents AS
SELECT
    CAST(year AS INTEGER) AS year,
    region AS region,
    CAST(inputProduceFiberMT AS REAL) AS inputProduceFiberMT,
    CAST(inputProduceResinMT AS REAL) AS inputProduceResinMT,
    CAST(inputImportResinMT AS REAL) AS inputImportResinMT,
    CAST(inputImportArticlesMT AS REAL) AS inputImportArticlesMT,
    CAST(inputImportGoodsMT AS REAL) AS inputImportGoodsMT,
    CAST(inputImportFiberMT AS REAL) AS inputImportFiberMT,
    CAST(inputAdditivesMT AS REAL) AS inputAdditivesMT,
    CAST(consumptionAgriculturePercent AS REAL) AS consumptionAgriculturePercent,
    CAST(consumptionConstructionPercent AS REAL) AS consumptionConstructionPercent,
    CAST(consumptionElectronicPercent AS REAL) AS consumptionElectronicPercent,
    CAST(consumptionHouseholdLeisureSportsPercent AS REAL) AS consumptionHouseholdLeisureSportsPercent,
    CAST(consumptionPackagingPercent AS REAL) AS consumptionPackagingPercent,
    CAST(consumptionTransporationPercent AS REAL) AS consumptionTransporationPercent,
    CAST(consumptionTextitlePercent AS REAL) AS consumptionTextitlePercent,
    CAST(consumptionOtherPercent AS REAL) AS consumptionOtherPercent,
    CAST(eolRecyclingPercent AS REAL) AS eolRecyclingPercent,
    CAST(eolIncinerationPercent AS REAL) AS eolIncinerationPercent,
    CAST(eolLandfillPercent AS REAL) AS eolLandfillPercent,
    CAST(eolMismanagedPercent AS REAL) AS eolMismanagedPercent
FROM
    file_extrapolatepercents
