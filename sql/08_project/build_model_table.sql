CREATE TABLE {table_name} AS
SELECT
    auxiliary_allowed.year AS year,
    auxiliary_allowed.region AS region,
    auxiliary_allowed.population AS population,
    auxiliary_allowed.gdp AS gdp,
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
    (
        SELECT
            year,
            region,
            population,
            gdp
        FROM
            auxiliary
        WHERE
            year >= 2007
    ) auxiliary_allowed
LEFT OUTER JOIN
    summary
ON
    auxiliary_allowed.year = summary.year
    AND auxiliary_allowed.region = summary.region
UNION ALL
SELECT
    year,
    region,
    NULL AS population,
    NULL AS gdp,
    consumptionAgricultureMT,
    consumptionConstructionMT,
    consumptionElectronicMT,
    consumptionHouseholdLeisureSportsMT,
    consumptionOtherMT,
    consumptionPackagingMT,
    consumptionTextileMT,
    consumptionTransporationMT,
    NULL AS eolRecyclingPercent,
    NULL AS eolIncinerationPercent,
    NULL AS eolLandfillPercent,
    NULL AS eolMismanagedPercent,
    NULL AS netImportArticlesPercent,
    NULL AS netImportFibersPercent,
    NULL AS netImportGoodsPercent,
    NULL AS netImportResinPercent,
    NULL AS netWasteTradePercent,
    NULL AS netImportArticlesMT,
    NULL AS netImportFibersMT,
    NULL AS netImportGoodsMT,
    NULL AS netImportResinMT,
    NULL AS netWasteTradeMT,
    NULL AS newWasteMT
FROM 
    raw_future