CREATE TABLE consumption_intermediate_waste AS
SELECT
    consumption_primary_restructure.year AS year,
    consumption_primary_restructure.region AS region,
    consumption_primary_restructure.consumptionAgricultureMT AS consumptionAgricultureMT,
    consumption_primary_restructure.consumptionConstructionMT AS consumptionConstructionMT,
    consumption_primary_restructure.consumptionElectronicMT AS consumptionElectronicMT,
    consumption_primary_restructure.consumptionHouseholdLeisureSportsMT AS consumptionHouseholdLeisureSportsMT,
    consumption_primary_restructure.consumptionPackagingMT AS consumptionPackagingMT,
    consumption_primary_restructure.consumptionTransportationMT AS consumptionTransportationMT,
    consumption_primary_restructure.consumptionTextileMT AS consumptionTextileMT,
    consumption_primary_restructure.consumptionOtherMT AS consumptionOtherMT,
    consumption_primary_restructure.eolRecyclingPercent AS eolRecyclingPercent,
    consumption_primary_restructure.eolIncinerationPercent AS eolIncinerationPercent,
    consumption_primary_restructure.eolLandfillPercent AS eolLandfillPercent,
    consumption_primary_restructure.eolMismanagedPercent AS eolMismanagedPercent,
    consumption_primary_restructure.netImportArticlesMT AS netImportArticlesMT,
    consumption_primary_restructure.netImportFibersMT AS netImportFibersMT,
    consumption_primary_restructure.netImportGoodsMT AS netImportGoodsMT,
    consumption_primary_restructure.netImportResinMT AS netImportResinMT,
    consumption_primary_restructure.netWasteTradeMT AS netWasteTradeMT,
    0 AS newWasteMT
FROM
    consumption_primary_restructure