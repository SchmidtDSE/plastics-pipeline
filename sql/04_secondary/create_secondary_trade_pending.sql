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
                    WHEN region = 'china' THEN 0.7298245614
                    WHEN region = 'eu30' THEN 0.7
                    WHEN region = 'nafta' THEN 0.6857142857
                    WHEN region = 'row' THEN 0.725
                    ELSE NULL
                END
            ) AS consumptionAgricultureMT,
            newWasteMT * eolRecyclingPercent * consumptionConstructionPercent * (
                CASE
                    WHEN region = 'china' THEN 0.6777777778
                    WHEN region = 'eu30' THEN 0.6660098522
                    WHEN region = 'nafta' THEN 0.6247191011
                    WHEN region = 'row' THEN 0.656
                    ELSE NULL
                END
            ) AS consumptionConstructionMT,
            newWasteMT * eolRecyclingPercent * consumptionElectronicPercent * (
                CASE
                    WHEN region = 'china' THEN 0.6709677419
                    WHEN region = 'eu30' THEN 0.6580645161
                    WHEN region = 'nafta' THEN 0.7111111111
                    WHEN region = 'row' THEN 0.6769230769
                    ELSE NULL
                END
            ) AS consumptionElectronicMT,
            newWasteMT * eolRecyclingPercent * consumptionHouseholdLeisureSportsPercent * (
                CASE
                    WHEN region = 'china' THEN 0.335042735
                    WHEN region = 'eu30' THEN 0.4831683168
                    WHEN region = 'nafta' THEN 0.6614718615
                    WHEN region = 'row' THEN 0.5386666667
                    ELSE NULL
                END
            ) AS consumptionHouseholdLeisureSportsMT,
            newWasteMT * eolRecyclingPercent * consumptionOtherPercent * (
                CASE
                    WHEN region = 'china' THEN 0.3733333333
                    WHEN region = 'eu30' THEN 0.5064220183
                    WHEN region = 'nafta' THEN 0.4056338028
                    WHEN region = 'row' THEN 0.4423529412
                    ELSE NULL
                END
            ) AS consumptionOtherMT,
            newWasteMT * eolRecyclingPercent * consumptionPackagingPercent * (
                CASE
                    WHEN region = 'china' THEN 0.7883381924
                    WHEN region = 'eu30' THEN 0.7900990099
                    WHEN region = 'nafta' THEN 0.7857142857
                    WHEN region = 'row' THEN 0.7873684211
                    ELSE NULL
                END
            ) AS consumptionPackagingMT,
            newWasteMT * eolRecyclingPercent * consumptionTextilePercent * (
                CASE
                    WHEN region = 'china' THEN 0.8
                    WHEN region = 'eu30' THEN 0.8
                    WHEN region = 'nafta' THEN 0.8
                    WHEN region = 'row' THEN 0.8
                    ELSE NULL
                END
            ) AS consumptionTextileMT,
            newWasteMT * eolRecyclingPercent * consumptionTransportationPercent * (
                CASE
                    WHEN region = 'china' THEN 0.64
                    WHEN region = 'eu30' THEN 0.6344827586
                    WHEN region = 'nafta' THEN 0.5546666667
                    WHEN region = 'row' THEN 0.6105263158
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