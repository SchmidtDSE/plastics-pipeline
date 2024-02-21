CREATE VIEW consumption_secondary_no_displace AS
SELECT
    with_shares.year AS year,
    with_shares.region AS region,
    with_shares.consumptionAgricultureMT * (1 - with_shares.shareExports) + total_exports.consumptionAgricultureMT * with_shares.shareImports AS consumptionAgricultureMT,
    with_shares.consumptionConstructionMT * (1 - with_shares.shareExports) + total_exports.consumptionConstructionMT * with_shares.shareImports AS consumptionConstructionMT,
    with_shares.consumptionElectronicMT * (1 - with_shares.shareExports) + total_exports.consumptionElectronicMT * with_shares.shareImports AS consumptionElectronicMT,
    with_shares.consumptionHouseholdLeisureSportsMT * (1 - with_shares.shareExports) + total_exports.consumptionHouseholdLeisureSportsMT * with_shares.shareImports AS consumptionHouseholdLeisureSportsMT,
    with_shares.consumptionPackagingMT * (1 - with_shares.shareExports) + total_exports.consumptionPackagingMT * with_shares.shareImports AS consumptionPackagingMT,
    with_shares.consumptionTransportationMT * (1 - with_shares.shareExports) + total_exports.consumptionTransportationMT * with_shares.shareImports AS consumptionTransportationMT,
    with_shares.consumptionTextileMT * (1 - with_shares.shareExports) + total_exports.consumptionTextileMT * with_shares.shareImports AS consumptionTextileMT,
    with_shares.consumptionOtherMT * (1 - with_shares.shareExports) + total_exports.consumptionOtherMT * with_shares.shareImports AS consumptionOtherMT
FROM
    (
        SELECT
            consumption_secondary_trade_pending.year AS year,
            consumption_secondary_trade_pending.region AS region,
            consumption_secondary_trade_pending.consumptionAgricultureMT AS consumptionAgricultureMT,
            consumption_secondary_trade_pending.consumptionConstructionMT AS consumptionConstructionMT,
            consumption_secondary_trade_pending.consumptionElectronicMT AS consumptionElectronicMT,
            consumption_secondary_trade_pending.consumptionHouseholdLeisureSportsMT AS consumptionHouseholdLeisureSportsMT,
            consumption_secondary_trade_pending.consumptionPackagingMT AS consumptionPackagingMT,
            consumption_secondary_trade_pending.consumptionTransportationMT AS consumptionTransportationMT,
            consumption_secondary_trade_pending.consumptionTextileMT AS consumptionTextileMT,
            consumption_secondary_trade_pending.consumptionOtherMT AS consumptionOtherMT,
            (
                CASE
                    WHEN consumption_secondary_trade_pending.productionTradePercent > 0 THEN consumption_secondary_trade_pending.productionTradePercent / totalImports.productionTradePercent
                    ELSE 0
                END
            ) AS shareImports,
            (
                CASE
                    WHEN consumption_secondary_trade_pending.productionTradePercent <= 0 THEN -1 * consumption_secondary_trade_pending.productionTradePercent
                    ELSE 0
                END
            ) AS shareExports
        FROM
            consumption_secondary_trade_pending
        INNER JOIN
            (
                SELECT
                    year,
                    sum(productionTradePercent) AS productionTradePercent
                FROM
                    consumption_secondary_trade_pending
                WHERE
                    productionTradePercent > 0
                GROUP BY
                    year
            ) totalImports
        ON
            totalImports.year = consumption_secondary_trade_pending.year
    ) with_shares
INNER JOIN
    (
        SELECT
            year,
            SUM(consumptionAgricultureMT * productionTradePercent * -1) AS consumptionAgricultureMT,
            SUM(consumptionConstructionMT * productionTradePercent * -1) AS consumptionConstructionMT,
            SUM(consumptionElectronicMT * productionTradePercent * -1) AS consumptionElectronicMT,
            SUM(consumptionHouseholdLeisureSportsMT * productionTradePercent * -1) AS consumptionHouseholdLeisureSportsMT,
            SUM(consumptionPackagingMT * productionTradePercent * -1) AS consumptionPackagingMT,
            SUM(consumptionTransportationMT * productionTradePercent * -1) AS consumptionTransportationMT,
            SUM(consumptionTextileMT * productionTradePercent * -1) AS consumptionTextileMT,
            SUM(consumptionOtherMT * productionTradePercent * -1) AS consumptionOtherMT
        FROM
            consumption_secondary_trade_pending
        WHERE
            productionTradePercent <= 0
        GROUP BY
            year
    ) total_exports
ON
    with_shares.year = total_exports.year