CREATE TABLE {{table_name}} AS
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
    summary.consumptionTransportationMT AS consumptionTransportationMT,
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
        summary.consumptionTransportationMT
    ) AS netImportArticlesPercent,
    summary.netImportFibersMT / (
        summary.consumptionAgricultureMT +
        summary.consumptionConstructionMT +
        summary.consumptionElectronicMT +
        summary.consumptionHouseholdLeisureSportsMT +
        summary.consumptionOtherMT +
        summary.consumptionPackagingMT +
        summary.consumptionTextileMT +
        summary.consumptionTransportationMT
    ) AS netImportFibersPercent,
    summary.netImportGoodsMT / (
        summary.consumptionAgricultureMT +
        summary.consumptionConstructionMT +
        summary.consumptionElectronicMT +
        summary.consumptionHouseholdLeisureSportsMT +
        summary.consumptionOtherMT +
        summary.consumptionPackagingMT +
        summary.consumptionTextileMT +
        summary.consumptionTransportationMT
    ) AS netImportGoodsPercent,
    summary.netImportResinMT / (
        summary.consumptionAgricultureMT +
        summary.consumptionConstructionMT +
        summary.consumptionElectronicMT +
        summary.consumptionHouseholdLeisureSportsMT +
        summary.consumptionOtherMT +
        summary.consumptionPackagingMT +
        summary.consumptionTextileMT +
        summary.consumptionTransportationMT
    ) AS netImportResinPercent,
    summary.netWasteTradeMT / (
        summary.consumptionAgricultureMT +
        summary.consumptionConstructionMT +
        summary.consumptionElectronicMT +
        summary.consumptionHouseholdLeisureSportsMT +
        summary.consumptionOtherMT +
        summary.consumptionPackagingMT +
        summary.consumptionTextileMT +
        summary.consumptionTransportationMT
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
