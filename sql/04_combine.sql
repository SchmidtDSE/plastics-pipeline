CREATE VIEW overview AS
SELECT
    with_trade.year AS year,
    with_trade.region AS region,
    with_trade.eolRecyclingMT AS eolRecyclingMT,
    with_trade.eolLandfillMT AS eolLandfillMT,
    with_trade.eolIncinerationMT AS eolIncinerationMT,
    with_trade.eolMismanagedMT AS eolMismanagedMT,
    with_trade.consumptionAgricultureMT AS consumptionAgricultureMT,
    with_trade.consumptionConstructionMT AS consumptionConstructionMT,
    with_trade.consumptionElectronicMT AS consumptionElectronicMT,
    with_trade.consumptionHouseholdLeisureSportsMT AS consumptionHouseholdLeisureSportsMT,
    with_trade.consumptionPackagingMT AS consumptionPackagingMT,
    with_trade.consumptionTransporationMT AS consumptionTransporationMT,
    with_trade.consumptionTextitleMT AS consumptionTextitleMT,
    with_trade.consumptionOtherMT AS consumptionOtherMT,
    (
        CASE
            WHEN with_trade.netImportsMT > 0 THEN with_trade.netImportsMT
            ELSE 0
        END
    ) AS netImportsMT,
    (
        CASE
            WHEN with_trade.netImportsMT < 0 THEN with_trade.netImportsMT
            ELSE 0
        END
    ) AS netExportsMT,
    with_trade.totalConsumption + with_trade.netImportsMT AS domesticProductionMT
FROM
    (
        SELECT
            no_trade.year AS year,
            no_trade.region AS region,
            no_trade.eolRecyclingMT AS eolRecyclingMT,
            no_trade.eolLandfillMT AS eolLandfillMT,
            no_trade.eolIncinerationMT AS eolIncinerationMT,
            no_trade.eolMismanagedMT AS eolMismanagedMT,
            no_trade.consumptionAgricultureMT AS consumptionAgricultureMT,
            no_trade.consumptionConstructionMT AS consumptionConstructionMT,
            no_trade.consumptionElectronicMT AS consumptionElectronicMT,
            no_trade.consumptionHouseholdLeisureSportsMT AS consumptionHouseholdLeisureSportsMT,
            no_trade.consumptionPackagingMT AS consumptionPackagingMT,
            no_trade.consumptionTransporationMT AS consumptionTransporationMT,
            no_trade.consumptionTextitleMT AS consumptionTextitleMT,
            no_trade.consumptionOtherMT AS consumptionOtherMT,
            no_trade.totalConsumption AS totalConsumption,
            no_trade.totalConsumption * percentNetImports AS netImportsMT
        FROM
            (
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
                    consumption_overview.consumptionOtherMT AS consumptionOtherMT,
                    (
                        consumption_overview.consumptionAgricultureMT + 
                        consumption_overview.consumptionConstructionMT + 
                        consumption_overview.consumptionElectronicMT + 
                        consumption_overview.consumptionHouseholdLeisureSportsMT + 
                        consumption_overview.consumptionPackagingMT + 
                        consumption_overview.consumptionTransporationMT + 
                        consumption_overview.consumptionTextitleMT + 
                        consumption_overview.consumptionOtherMT
                    ) AS totalConsumption
                FROM
                    eol_overview
                INNER JOIN
                    consumption_overview
                ON
                    eol_overview.year = consumption_overview.year
                    AND eol_overview.region = consumption_overview.region
            ) no_trade
        INNER JOIN
            (
                SELECT
                    region,
                    percentNetImports
                FROM
                    net_imports_percent
                WHERE
                    year = 2020
            ) final_net_imports
        ON
            no_trade.region = final_net_imports.region
    ) with_trade
