CREATE VIEW overview AS
SELECT
    eol_overview.year AS year,
    eol_overview.region AS region,
    eol_overview.eolRecyclingMT AS eolRecyclingMT,
    eol_overview.eolLandfillMT AS eolLandfillMT,
    eol_overview.eolIncinerationMT AS eolIncinerationMT,
    eol_overview.eolMismanagedMT AS eolMismanagedMT,
    consumption_overview.consumptionAgricultureMT AS consumptionAgricultureMT,
    consumption_overview.consumptionConstructionMT AS consumptionConstructionMT,
    consumption_overview.consumptionElectronicMT AS consumptionElectronicMT,
    consumption_overview.consumptionHouseholdLeisureSportsMT AS consumptionHouseholdLeisureSportsMT,
    consumption_overview.consumptionPackagingMT AS consumptionPackagingMT,
    consumption_overview.consumptionTransporationMT AS consumptionTransporationMT,
    consumption_overview.consumptionTextitleMT AS consumptionTextitleMT,
    consumption_overview.consumptionOtherMT AS consumptionOtherMT
FROM
    eol_overview
INNER JOIN
    consumption_overview
ON
    eol_overview.year = consumption_overview.year
    AND eol_overview.region = consumption_overview.region;
