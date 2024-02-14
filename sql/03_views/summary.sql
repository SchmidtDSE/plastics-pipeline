CREATE VIEW summary AS
SELECT
    without_waste_trade.year AS year,
    without_waste_trade.region AS region,
    without_waste_trade.inputProduceFiberMT AS inputProduceFiberMT,
    without_waste_trade.inputProduceResinMT AS inputProduceResinMT,
    without_waste_trade.inputImportResinMT AS inputImportResinMT,
    without_waste_trade.inputImportArticlesMT AS inputImportArticlesMT,
    without_waste_trade.inputImportGoodsMT AS inputImportGoodsMT,
    without_waste_trade.inputImportFiberMT AS inputImportFiberMT,
    without_waste_trade.inputAdditivesMT AS inputAdditivesMT,
    without_waste_trade.netImportArticlesMT AS netImportArticlesMT,
    without_waste_trade.netImportFibersMT AS netImportFibersMT,
    without_waste_trade.netImportGoodsMT AS netImportGoodsMT,
    without_waste_trade.netImportResinMT AS netImportResinMT,
    net_waste_trade.netMT AS netWasteTradeMT,
    without_waste_trade.consumptionAgricultureMT AS consumptionAgricultureMT,
    without_waste_trade.consumptionConstructionMT AS consumptionConstructionMT,
    without_waste_trade.consumptionElectronicMT AS consumptionElectronicMT,
    without_waste_trade.consumptionHouseholdLeisureSportsMT AS consumptionHouseholdLeisureSportsMT,
    without_waste_trade.consumptionPackagingMT AS consumptionPackagingMT,
    without_waste_trade.consumptionTransportationMT AS consumptionTransportationMT,
    without_waste_trade.consumptionTextileMT AS consumptionTextileMT,
    without_waste_trade.consumptionOtherMT AS consumptionOtherMT,
    without_waste_trade.eolRecyclingPercent AS eolRecyclingPercent,
    without_waste_trade.eolIncinerationPercent AS eolIncinerationPercent,
    without_waste_trade.eolLandfillPercent AS eolLandfillPercent,
    without_waste_trade.eolMismanagedPercent AS eolMismanagedPercent
FROM
    (
        SELECT
            without_net_imports.year AS year,
            without_net_imports.region AS region,
            without_net_imports.inputProduceFiberMT AS inputProduceFiberMT,
            without_net_imports.inputProduceResinMT AS inputProduceResinMT,
            without_net_imports.inputImportResinMT AS inputImportResinMT,
            without_net_imports.inputImportArticlesMT AS inputImportArticlesMT,
            without_net_imports.inputImportGoodsMT AS inputImportGoodsMT,
            without_net_imports.inputImportFiberMT AS inputImportFiberMT,
            without_net_imports.inputAdditivesMT AS inputAdditivesMT,
            overview_net_imports.articlesNetMT AS netImportArticlesMT,
            overview_net_imports.fibersNetMT AS netImportFibersMT,
            overview_net_imports.goodsNetMT AS netImportGoodsMT,
            overview_net_imports.resinNetMT AS netImportResinMT,
            without_net_imports.consumptionAgricultureMT AS consumptionAgricultureMT,
            without_net_imports.consumptionConstructionMT AS consumptionConstructionMT,
            without_net_imports.consumptionElectronicMT AS consumptionElectronicMT,
            without_net_imports.consumptionHouseholdLeisureSportsMT AS consumptionHouseholdLeisureSportsMT,
            without_net_imports.consumptionPackagingMT AS consumptionPackagingMT,
            without_net_imports.consumptionTransportationMT AS consumptionTransportationMT,
            without_net_imports.consumptionTextileMT AS consumptionTextileMT,
            without_net_imports.consumptionOtherMT AS consumptionOtherMT,
            without_net_imports.eolRecyclingPercent AS eolRecyclingPercent,
            without_net_imports.eolIncinerationPercent AS eolIncinerationPercent,
            without_net_imports.eolLandfillPercent AS eolLandfillPercent,
            without_net_imports.eolMismanagedPercent AS eolMismanagedPercent
        FROM
            (
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
                    inputs_and_consumption.consumptionTransportationMT AS consumptionTransportationMT,
                    inputs_and_consumption.consumptionTextileMT AS consumptionTextileMT,
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
                            overview_consumption.transportationMT AS consumptionTransportationMT,
                            overview_consumption.textileMT AS consumptionTextileMT,
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
            ) without_net_imports
        LEFT OUTER JOIN
            overview_net_imports
        ON
            overview_net_imports.year = without_net_imports.year
            AND overview_net_imports.region = without_net_imports.region
    ) without_waste_trade
LEFT OUTER JOIN
    net_waste_trade
ON
    net_waste_trade.year = without_waste_trade.year
    AND net_waste_trade.region = without_waste_trade.region
