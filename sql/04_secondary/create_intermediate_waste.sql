CREATE TABLE consumption_intermediate_waste AS
SELECT
    consumption_primary_restructure.year AS year,
    consumption_primary_restructure.region AS region,
    consumption_primary_restructure.agricultureMT AS agricultureMT,
    consumption_primary_restructure.constructionMT AS constructionMT,
    consumption_primary_restructure.electronicMT AS electronicMT,
    consumption_primary_restructure.householdLeisureSportsMT AS householdLeisureSportsMT,
    consumption_primary_restructure.packagingMT AS packagingMT,
    consumption_primary_restructure.transportationMT AS transportationMT,
    consumption_primary_restructure.textileMT AS textileMT,
    consumption_primary_restructure.otherMT AS otherMT,
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