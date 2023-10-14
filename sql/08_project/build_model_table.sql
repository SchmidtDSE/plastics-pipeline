CREATE TABLE {table_name} AS
SELECT
    auxiliary.year AS year,
    auxiliary.region AS region,
    auxiliary.population AS population,
    auxiliary.gdp AS gdp,
    summary.consumptionAgricultureMT AS consumptionAgricultureMT,
    summary.consumptionConstructionMT AS consumptionConstructionMT,
    summary.consumptionElectronicMT AS consumptionElectronicMT,
    summary.consumptionHouseholdLeisureSportsMT AS consumptionHouseholdLeisureSportsMT,
    summary.consumptionOtherMT AS consumptionOtherMT,
    summary.consumptionPackagingMT AS consumptionPackagingMT,
    summary.consumptionTextileMT AS consumptionTextileMT,
    summary.consumptionTransporationMT AS consumptionTransporationMT,
    summary.eolRecyclingPercent AS eolRecyclingPercent,
    summary.eolIncinerationPercent AS eolIncinerationPercent,
    summary.eolLandfillPercent AS eolLandfillPercent,
    summary.eolMismanagedPercent AS eolMismanagedPercent,
    summary.netImportArticlesMT / (
        summary.consumptionAgricultureMT +
        summary.consumptionConstructionMT +
        summary.consumptionElectronicMT +
        summary.consumptionHouseholdLeisureSportsMT +
        summary.consumptionOtherMT +
        summary.consumptionPackagingMT +
        summary.consumptionTextileMT +
        summary.consumptionTransporationMT
    ) AS netImportArticlesPercent,
    summary.netImportFibersMT / (
        summary.consumptionAgricultureMT +
        summary.consumptionConstructionMT +
        summary.consumptionElectronicMT +
        summary.consumptionHouseholdLeisureSportsMT +
        summary.consumptionOtherMT +
        summary.consumptionPackagingMT +
        summary.consumptionTextileMT +
        summary.consumptionTransporationMT
    ) AS netImportFibersPercent,
    summary.netImportGoodsMT / (
        summary.consumptionAgricultureMT +
        summary.consumptionConstructionMT +
        summary.consumptionElectronicMT +
        summary.consumptionHouseholdLeisureSportsMT +
        summary.consumptionOtherMT +
        summary.consumptionPackagingMT +
        summary.consumptionTextileMT +
        summary.consumptionTransporationMT
    ) AS netImportGoodsPercent,
    summary.netImportResinMT / (
        summary.consumptionAgricultureMT +
        summary.consumptionConstructionMT +
        summary.consumptionElectronicMT +
        summary.consumptionHouseholdLeisureSportsMT +
        summary.consumptionOtherMT +
        summary.consumptionPackagingMT +
        summary.consumptionTextileMT +
        summary.consumptionTransporationMT
    ) AS netImportResinPercent,
    summary.netWasteTradeMT / (
        summary.consumptionAgricultureMT +
        summary.consumptionConstructionMT +
        summary.consumptionElectronicMT +
        summary.consumptionHouseholdLeisureSportsMT +
        summary.consumptionOtherMT +
        summary.consumptionPackagingMT +
        summary.consumptionTextileMT +
        summary.consumptionTransporationMT
    ) AS netWasteTradePercent,
    netImportArticlesMT AS netImportArticlesMT,
    netImportFibersMT AS netImportFibersMT,
    netImportGoodsMT AS netImportGoodsMT,
    netImportResinMT AS netImportResinMT,
    netWasteTradeMT AS netWasteTradeMT,
    NULL AS newWasteMT
FROM
    auxiliary
LEFT OUTER JOIN
    summary
ON
    auxiliary.year = summary.year
    AND auxiliary.region = summary.region