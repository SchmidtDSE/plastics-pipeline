CREATE VIEW raw_future AS
SELECT
    CAST(year AS INTEGER) AS year,
    region AS region,
    CAST(consumptionAgricultureMT AS REAL) AS consumptionAgricultureMT,
    CAST(consumptionConstructionMT AS REAL) AS consumptionConstructionMT,
    CAST(consumptionElectronicMT AS REAL) AS consumptionElectronicMT,
    CAST(consumptionHouseholdLeisureSportsMT AS REAL) AS consumptionHouseholdLeisureSportsMT,
    CAST(consumptionOtherMT AS REAL) AS consumptionOtherMT,
    CAST(consumptionPackagingMT AS REAL) AS consumptionPackagingMT,
    CAST(consumptionTextileMT AS REAL) AS consumptionTextileMT,
    CAST(consumptionTransporationMT AS REAL) AS consumptionTransporationMT
FROM
    file_23historic