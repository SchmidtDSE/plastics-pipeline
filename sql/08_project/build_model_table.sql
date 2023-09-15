CREATE TABLE {table_name} AS
SELECT
    auxiliary.year AS year,
    summary.region AS region,
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
    summary.netImportArticlesMT AS netImportArticlesMT,
    summary.netImportFibersMT AS netImportFibersMT,
    summary.netImportGoodsMT AS netImportGoodsMT,
    summary.netImportResinMT AS netImportResinMT
FROM
    auxiliary
LEFT OUTER JOIN
    summary
ON
    auxiliary.year = summary.year
    AND auxiliary.region = summary.region