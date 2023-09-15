SELECT
    year,
    region,
    eolRecyclingMT,
    eolLandfillMT,
    eolIncinerationMT,
    eolMismanagedMT,
    consumptionAgricultureMT,
    consumptionConstructionMT,
    consumptionElectronicMT,
    consumptionHouseholdLeisureSportsMT,
    consumptionPackagingMT,
    consumptionTransporationMT,
    consumptionTextileMT,
    consumptionOtherMT,
    (
        CASE
            WHEN netImport > 0 THEN netImport
            ELSE 0
        END
    ) AS netImportsMT,
    (
        CASE
            WHEN netExportsMT < 0 THEN -1 * netImport
            ELSE 0
        END
    ) AS netImportsMT,
    totalConsumption - netImport AS domesticProductionMT
FROM
    (
        SELECT
            year,
            region,
            eolRecyclingPercent * newWasteMT AS eolRecyclingMT,
            eolLandfillPercent * newWasteMT AS eolLandfillMT,
            eolIncinerationPercent * newWasteMT AS eolIncinerationMT,
            eolMismanagedPercent * newWasteMT AS eolMismanagedMT,
            consumptionAgricultureMT,
            consumptionConstructionMT,
            consumptionElectronicMT,
            consumptionHouseholdLeisureSportsMT,
            consumptionPackagingMT,
            consumptionTransporationMT,
            consumptionTextileMT,
            consumptionOtherMT,
            (
                netImportArticlesMT +
                netImportFibersMT +
                netImportGoodsMT +
                netImportResinMT
            ) AS netImport,
            (
                consumptionAgricultureMT +
                consumptionConstructionMT +
                consumptionElectronicMT +
                consumptionHouseholdLeisureSportsMT +
                consumptionPackagingMT +
                consumptionTransporationMT +
                consumptionTextileMT +
                consumptionOtherMT
            ) AS totalConsumption
        FROM
            project_curve
    ) metrics
