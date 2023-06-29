CREATE VIEW extrapolate AS
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
    CAST(consumptionAgricultureMT AS REAL) AS consumptionAgricultureMT,
    CAST(consumptionConstructionMT AS REAL) AS consumptionConstructionMT,
    CAST(consumptionElectronicMT AS REAL) AS consumptionElectronicMT,
    CAST(consumptionHouseholdLeisureSportsMT AS REAL) AS consumptionHouseholdLeisureSportsMT,
    CAST(consumptionPackagingMT AS REAL) AS consumptionPackagingMT,
    CAST(consumptionTransporationMT AS REAL) AS consumptionTransporationMT,
    CAST(consumptionTextitleMT AS REAL) AS consumptionTextitleMT,
    CAST(consumptionOtherMT AS REAL) AS consumptionOtherMT,
    CAST(eolRecyclingPercent AS REAL) AS eolRecyclingPercent,
    CAST(eolIncinerationPercent AS REAL) AS eolIncinerationPercent,
    CAST(eolLandfillPercent AS REAL) AS eolLandfillPercent,
    CAST(eolMismanagedPercent AS REAL) AS eolMismanagedPercent
FROM
    file_extrapolate
