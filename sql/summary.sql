CREATE VIEW summary AS
SELECT
    inputs_and_consumption.year AS year,
    inputs_and_consumption.region AS region,
    inputs_and_consumption.inputProduceFiberMT AS inputProduceFiberMT,
    inputs_and_consumption.inputProduceResinMT AS inputProduceResinMT,
    inputs_and_consumption.inputImportResinMT AS inputImportResinMT,
    inputs_and_consumption.inputImportArticlesMT AS inputImportArticlesMT,
    inputs_and_consumption.inputImportGoodsMT AS inputImportGoodsMT,
    inputs_and_consumption.inputImportFiberMT AS inputImportFiberMT,
    inputs_and_consumption.inputAdditivesMT AS inputAdditivesMT,
    inputs_and_consumption.consumptionAgricultureMT AS consumptionAgricultureMT,
    inputs_and_consumption.consumptionConstructionMT AS consumptionConstructionMT,
    inputs_and_consumption.consumptionElectronicMT AS consumptionElectronicMT,
    inputs_and_consumption.consumptionHouseholdLeisureSportsMT AS consumptionHouseholdLeisureSportsMT,
    inputs_and_consumption.consumptionPackagingMT AS consumptionPackagingMT,
    inputs_and_consumption.consumptionTransporationMT AS consumptionTransporationMT,
    inputs_and_consumption.consumptionTextitleMT AS consumptionTextitleMT,
    inputs_and_consumption.consumptionOtherMT AS consumptionOtherMT,
    overview_eol.recyclingPercent AS eolRecyclingPercent,
    overview_eol.incinerationPercent AS eolIncinerationPercent,
    overview_eol.landfillPercent AS eolLandfillPercent,
    overview_eol.mismanagedPercent AS eolMismanagedPercent
FROM
    (
        SELECT
            overview_inputs.year AS year,
            overview_inputs.region AS region,
            overview_inputs.produceFiberMT AS inputProduceFiberMT,
            overview_inputs.produceResinMT AS inputProduceResinMT,
            overview_inputs.importResinMT AS inputImportResinMT,
            overview_inputs.importArticlesMT AS inputImportArticlesMT,
            overview_inputs.importGoodsMT AS inputImportGoodsMT,
            overview_inputs.importFiberMT AS inputImportFiberMT,
            overview_inputs.additivesMT AS inputAdditivesMT,
            overview_consumption.agricultureMT AS consumptionAgricultureMT,
            overview_consumption.constructionMT AS consumptionConstructionMT,
            overview_consumption.electronicMT AS consumptionElectronicMT,
            overview_consumption.householdLeisureSportsMT AS consumptionHouseholdLeisureSportsMT,
            overview_consumption.packagingMT AS consumptionPackagingMT,
            overview_consumption.transporationMT AS consumptionTransporationMT,
            overview_consumption.textitleMT AS consumptionTextitleMT,
            overview_consumption.otherMT AS consumptionOtherMT
        FROM
            overview_inputs
        LEFT OUTER JOIN
            overview_consumption
        ON
            overview_inputs.year = overview_consumption.year
            AND overview_inputs.region = overview_consumption.region
    ) inputs_and_consumption
LEFT OUTER JOIN
    overview_eol
ON
    inputs_and_consumption.year = overview_eol.year
    AND inputs_and_consumption.region = overview_eol.region