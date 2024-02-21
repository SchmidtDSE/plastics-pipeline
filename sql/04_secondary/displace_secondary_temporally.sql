CREATE VIEW consumption_secondary_restructure AS
SELECT
    consumption_secondary_no_displace.year + 1 AS year,
    consumption_secondary_no_displace.region AS region,
    consumption_secondary_no_displace.consumptionAgricultureMT AS consumptionAgricultureMT,
    consumption_secondary_no_displace.consumptionConstructionMT AS consumptionConstructionMT,
    consumption_secondary_no_displace.consumptionElectronicMT AS consumptionElectronicMT,
    consumption_secondary_no_displace.consumptionHouseholdLeisureSportsMT AS consumptionHouseholdLeisureSportsMT,
    consumption_secondary_no_displace.consumptionPackagingMT AS consumptionPackagingMT,
    consumption_secondary_no_displace.consumptionTransportationMT AS consumptionTransportationMT,
    consumption_secondary_no_displace.consumptionTextileMT AS consumptionTextileMT,
    consumption_secondary_no_displace.consumptionOtherMT AS consumptionOtherMT
FROM
    consumption_secondary_no_displace