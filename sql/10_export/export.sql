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
    consumptionTransportationMT,
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
            WHEN netImport < 0 THEN -1 * netImport
            ELSE 0
        END
    ) AS netExportsMT,
    totalConsumption - netImport AS domesticProductionMT,
    (
        CASE
            WHEN netWasteTradeMT > 0 THEN netWasteTradeMT
            ELSE 0
        END
    ) AS netWasteExportMT,
    (
        CASE
            WHEN netWasteTradeMT < 0 THEN -1 * netWasteTradeMT
            ELSE 0
        END
    ) AS netWasteImportMT
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
            consumptionTransportationMT,
            consumptionTextileMT,
            consumptionOtherMT,
            (
                netImportArticlesMT +
                netImportFibersMT +
                netImportGoodsMT
            ) AS netImport,
            (
                consumptionAgricultureMT +
                consumptionConstructionMT +
                consumptionElectronicMT +
                consumptionHouseholdLeisureSportsMT +
                consumptionPackagingMT +
                consumptionTransportationMT +
                consumptionTextileMT +
                consumptionOtherMT
            ) AS totalConsumption,
            netWasteTradeMT AS netWasteTradeMT
        FROM
            {{table_name}}
        WHERE
            year >= 2010
    ) metrics
