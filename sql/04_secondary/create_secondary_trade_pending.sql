CREATE VIEW consumption_secondary_trade_pending AS
SELECT
    year,
    region,
    consumptionAgricultureMT / (
        consumptionAgricultureMT +
        consumptionConstructionMT +
        consumptionElectronicMT +
        consumptionHouseholdLeisureSportsMT +
        consumptionPackagingMT +
        consumptionTransportationMT +
        consumptionTextileMT +
        consumptionOtherMT
    ) * totalRecycling AS consumptionAgricultureMT,
    consumptionConstructionMT / (
        consumptionAgricultureMT +
        consumptionConstructionMT +
        consumptionElectronicMT +
        consumptionHouseholdLeisureSportsMT +
        consumptionPackagingMT +
        consumptionTransportationMT +
        consumptionTextileMT +
        consumptionOtherMT
    ) * totalRecycling AS consumptionConstructionMT,
    consumptionElectronicMT / (
        consumptionAgricultureMT +
        consumptionConstructionMT +
        consumptionElectronicMT +
        consumptionHouseholdLeisureSportsMT +
        consumptionPackagingMT +
        consumptionTransportationMT +
        consumptionTextileMT +
        consumptionOtherMT
    ) * totalRecycling AS consumptionElectronicMT,
    consumptionHouseholdLeisureSportsMT / (
        consumptionAgricultureMT +
        consumptionConstructionMT +
        consumptionElectronicMT +
        consumptionHouseholdLeisureSportsMT +
        consumptionPackagingMT +
        consumptionTransportationMT +
        consumptionTextileMT +
        consumptionOtherMT
    ) * totalRecycling AS consumptionHouseholdLeisureSportsMT,
    consumptionPackagingMT / (
        consumptionAgricultureMT +
        consumptionConstructionMT +
        consumptionElectronicMT +
        consumptionHouseholdLeisureSportsMT +
        consumptionPackagingMT +
        consumptionTransportationMT +
        consumptionTextileMT +
        consumptionOtherMT
    ) * totalRecycling AS consumptionPackagingMT,
    consumptionTransportationMT / (
        consumptionAgricultureMT +
        consumptionConstructionMT +
        consumptionElectronicMT +
        consumptionHouseholdLeisureSportsMT +
        consumptionPackagingMT +
        consumptionTransportationMT +
        consumptionTextileMT +
        consumptionOtherMT
    ) * totalRecycling AS consumptionTransportationMT,
    consumptionTextileMT / (
        consumptionAgricultureMT +
        consumptionConstructionMT +
        consumptionElectronicMT +
        consumptionHouseholdLeisureSportsMT +
        consumptionPackagingMT +
        consumptionTransportationMT +
        consumptionTextileMT +
        consumptionOtherMT
    ) * totalRecycling AS consumptionTextileMT,
    consumptionOtherMT / (
        consumptionAgricultureMT +
        consumptionConstructionMT +
        consumptionElectronicMT +
        consumptionHouseholdLeisureSportsMT +
        consumptionPackagingMT +
        consumptionTransportationMT +
        consumptionTextileMT +
        consumptionOtherMT
    ) * totalRecycling AS consumptionOtherMT,
    productionTradePercent AS productionTradePercent
FROM
    (
        SELECT
            year,
            region,
            newWasteMT * eolRecyclingPercent * consumptionAgriculturePercent * (
                CASE
                    {% for region in regions %}
                    WHEN region = '{{ region["key"] }}' THEN {{ region["constants"]["agricultureSecondary"] }}
                    {% endfor %}
                    ELSE NULL
                END
            ) AS consumptionAgricultureMT,
            newWasteMT * eolRecyclingPercent * consumptionConstructionPercent * (
                CASE
                    {% for region in regions %}
                    WHEN region = '{{ region["key"] }}' THEN {{ region["constants"]["constructionSecondary"] }}
                    {% endfor %}
                    ELSE NULL
                END
            ) AS consumptionConstructionMT,
            newWasteMT * eolRecyclingPercent * consumptionElectronicPercent * (
                CASE
                    {% for region in regions %}
                    WHEN region = '{{ region["key"] }}' THEN {{ region["constants"]["electronicSecondary"] }}
                    {% endfor %}
                    ELSE NULL
                END
            ) AS consumptionElectronicMT,
            newWasteMT * eolRecyclingPercent * consumptionHouseholdLeisureSportsPercent * (
                CASE
                    {% for region in regions %}
                    WHEN region = '{{ region["key"] }}' THEN {{ region["constants"]["hlsSecondary"] }}
                    {% endfor %}
                    ELSE NULL
                END
            ) AS consumptionHouseholdLeisureSportsMT,
            newWasteMT * eolRecyclingPercent * consumptionOtherPercent * (
                CASE
                    {% for region in regions %}
                    WHEN region = '{{ region["key"] }}' THEN {{ region["constants"]["otherSecondary"] }}
                    {% endfor %}
                    ELSE NULL
                END
            ) AS consumptionOtherMT,
            newWasteMT * eolRecyclingPercent * consumptionPackagingPercent * (
                CASE
                    {% for region in regions %}
                    WHEN region = '{{ region["key"] }}' THEN {{ region["constants"]["packagingSecondary"] }}
                    {% endfor %}
                    ELSE NULL
                END
            ) AS consumptionPackagingMT,
            newWasteMT * eolRecyclingPercent * consumptionTextilePercent * (
                CASE
                    {% for region in regions %}
                    WHEN region = '{{ region["key"] }}' THEN {{ region["constants"]["textileSecondary"] }}
                    {% endfor %}
                    ELSE NULL
                END
            ) AS consumptionTextileMT,
            newWasteMT * eolRecyclingPercent * consumptionTransportationPercent * (
                CASE
                    {% for region in regions %}
                    WHEN region = '{{ region["key"] }}' THEN {{ region["constants"]["transportationSecondary"] }}
                    {% endfor %}
                    ELSE NULL
                END
            ) AS consumptionTransportationMT,
            productionTradePercent AS productionTradePercent,
            newWasteMT * eolRecyclingPercent * (1 - 0.2) AS totalRecycling
        FROM
            (
                SELECT
                    year,
                    region,
                    consumptionAgricultureMT / (
                        consumptionAgricultureMT +
                        consumptionConstructionMT +
                        consumptionElectronicMT +
                        consumptionHouseholdLeisureSportsMT +
                        consumptionPackagingMT +
                        consumptionTransportationMT +
                        consumptionTextileMT +
                        consumptionOtherMT
                    ) AS consumptionAgriculturePercent,
                    consumptionConstructionMT / (
                        consumptionAgricultureMT +
                        consumptionConstructionMT +
                        consumptionElectronicMT +
                        consumptionHouseholdLeisureSportsMT +
                        consumptionPackagingMT +
                        consumptionTransportationMT +
                        consumptionTextileMT +
                        consumptionOtherMT
                    ) AS consumptionConstructionPercent,
                    consumptionElectronicMT / (
                        consumptionAgricultureMT +
                        consumptionConstructionMT +
                        consumptionElectronicMT +
                        consumptionHouseholdLeisureSportsMT +
                        consumptionPackagingMT +
                        consumptionTransportationMT +
                        consumptionTextileMT +
                        consumptionOtherMT
                    ) AS consumptionElectronicPercent,
                    consumptionHouseholdLeisureSportsMT / (
                        consumptionAgricultureMT +
                        consumptionConstructionMT +
                        consumptionElectronicMT +
                        consumptionHouseholdLeisureSportsMT +
                        consumptionPackagingMT +
                        consumptionTransportationMT +
                        consumptionTextileMT +
                        consumptionOtherMT
                    ) AS consumptionHouseholdLeisureSportsPercent,
                    consumptionPackagingMT / (
                        consumptionAgricultureMT +
                        consumptionConstructionMT +
                        consumptionElectronicMT +
                        consumptionHouseholdLeisureSportsMT +
                        consumptionPackagingMT +
                        consumptionTransportationMT +
                        consumptionTextileMT +
                        consumptionOtherMT
                    ) AS consumptionPackagingPercent,
                    consumptionTransportationMT / (
                        consumptionAgricultureMT +
                        consumptionConstructionMT +
                        consumptionElectronicMT +
                        consumptionHouseholdLeisureSportsMT +
                        consumptionPackagingMT +
                        consumptionTransportationMT +
                        consumptionTextileMT +
                        consumptionOtherMT
                    ) AS consumptionTransportationPercent,
                    consumptionTextileMT / (
                        consumptionAgricultureMT +
                        consumptionConstructionMT +
                        consumptionElectronicMT +
                        consumptionHouseholdLeisureSportsMT +
                        consumptionPackagingMT +
                        consumptionTransportationMT +
                        consumptionTextileMT +
                        consumptionOtherMT
                    ) AS consumptionTextilePercent,
                    consumptionOtherMT / (
                        consumptionAgricultureMT +
                        consumptionConstructionMT +
                        consumptionElectronicMT +
                        consumptionHouseholdLeisureSportsMT +
                        consumptionPackagingMT +
                        consumptionTransportationMT +
                        consumptionTextileMT +
                        consumptionOtherMT
                    ) AS consumptionOtherPercent,
                    netWasteTradeMT / newWasteMT AS wasteTradePercent,
                    (
                        netImportArticlesMT +
                        netImportFibersMT +
                        netImportGoodsMT
                    ) / (
                        consumptionAgricultureMT +
                        consumptionConstructionMT +
                        consumptionElectronicMT +
                        consumptionHouseholdLeisureSportsMT +
                        consumptionPackagingMT +
                        consumptionTransportationMT +
                        consumptionTextileMT +
                        consumptionOtherMT
                    ) AS productionTradePercent,
                    eolRecyclingPercent AS eolRecyclingPercent,
                    newWasteMT
                FROM
                    consumption_intermediate_waste
            ) consumption_intermediate_waste_percents
    ) unadjusted