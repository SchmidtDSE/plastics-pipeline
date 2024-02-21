INSERT INTO consumption_primary_restructure (
    year,
    region,
    consumptionAgricultureMT,
    consumptionConstructionMT,
    consumptionElectronicMT,
    consumptionHouseholdLeisureSportsMT,
    consumptionPackagingMT,
    consumptionTransportationMT,
    consumptionTextileMT,
    consumptionOtherMT,
    eolRecyclingPercent,
    eolIncinerationPercent,
    eolLandfillPercent,
    eolMismanagedPercent,
    netImportArticlesMT,
    netImportFibersMT,
    netImportGoodsMT,
    netImportResinMT,
    netWasteTradeMT
)
SELECT
    year,
    region,
    (
        CASE
            WHEN consumptionAgricultureMT IS NULL THEN 0
            ELSE consumptionAgricultureMT
        END
    ) AS consumptionAgricultureMT,
    (
        CASE
            WHEN consumptionConstructionMT IS NULL THEN 0
            ELSE consumptionConstructionMT
        END
    ) AS consumptionConstructionMT,
    (
        CASE
            WHEN consumptionElectronicMT IS NULL THEN 0
            ELSE consumptionElectronicMT
        END
    ) AS consumptionElectronicMT,
    (
        CASE
            WHEN consumptionHouseholdLeisureSportsMT IS NULL THEN 0
            ELSE consumptionHouseholdLeisureSportsMT
        END
    ) AS consumptionHouseholdLeisureSportsMT,
    (
        CASE
            WHEN consumptionPackagingMT IS NULL THEN 0
            ELSE consumptionPackagingMT
        END
    ) AS consumptionPackagingMT,
    (
        CASE
            WHEN consumptionTransportationMT IS NULL THEN 0
            ELSE consumptionTransportationMT
        END
    ) AS consumptionTransportationMT,
    (
        CASE
            WHEN consumptionTextileMT IS NULL THEN 0
            ELSE consumptionTextileMT
        END
    ) AS consumptionTextileMT,
    (
        CASE
            WHEN consumptionOtherMT IS NULL THEN 0
            ELSE consumptionOtherMT
        END
    ) AS consumptionOtherMT,
    (
        CASE
            WHEN eolRecyclingPercent IS NULL THEN 0
            ELSE eolRecyclingPercent
        END
    ) AS eolRecyclingPercent,
    (
        CASE
            WHEN eolIncinerationPercent IS NULL THEN 0
            ELSE eolIncinerationPercent
        END
    ) AS eolIncinerationPercent,
    (
        CASE
            WHEN eolLandfillPercent IS NULL THEN 0
            ELSE eolLandfillPercent
        END
    ) AS eolLandfillPercent,
    (
        CASE
            WHEN eolMismanagedPercent IS NULL THEN 0
            ELSE eolMismanagedPercent
        END
    ) AS eolMismanagedPercent,
    (
        CASE
            WHEN netImportArticlesMT IS NULL THEN 0
            ELSE netImportArticlesMT
        END
    ) AS netImportArticlesMT,
    (
        CASE
            WHEN netImportFibersMT IS NULL THEN 0
            ELSE netImportFibersMT
        END
    ) AS netImportFibersMT,
    (
        CASE
            WHEN netImportGoodsMT IS NULL THEN 0
            ELSE netImportGoodsMT
        END
    ) AS netImportGoodsMT,
    (
        CASE
            WHEN netImportResinMT IS NULL THEN 0
            ELSE netImportResinMT
        END
    ) AS netImportResinMT,
    (
        CASE
            WHEN netWasteTradeMT IS NULL THEN 0
            ELSE netWasteTradeMT
        END
    ) AS netWasteTradeMT
FROM
    consumption_secondary_restructure