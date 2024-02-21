CREATE VIEW consumption_primary_restructure_no_historic AS
SELECT
    with_null.year AS year,
    with_null.region AS region,
    with_null.consumptionAgricultureMT AS consumptionAgricultureMT,
    with_null.consumptionConstructionMT AS consumptionConstructionMT,
    with_null.consumptionElectronicMT AS consumptionElectronicMT,
    with_null.consumptionHouseholdLeisureSportsMT AS consumptionHouseholdLeisureSportsMT,
    with_null.consumptionPackagingMT AS consumptionPackagingMT,
    with_null.consumptionTransportationMT AS consumptionTransportationMT,
    with_null.consumptionTextileMT AS consumptionTextileMT,
    with_null.consumptionOtherMT AS consumptionOtherMT,
    with_null.eolRecyclingPercent AS eolRecyclingPercent,
    with_null.eolIncinerationPercent AS eolIncinerationPercent,
    with_null.eolLandfillPercent AS eolLandfillPercent,
    with_null.eolMismanagedPercent AS eolMismanagedPercent,
    (
        CASE
            WHEN with_null.netImportArticlesMT IS NULL THEN 0
            ELSE with_null.netImportArticlesMT
        END
    ) AS netImportArticlesMT,
    (
        CASE
            WHEN with_null.netImportFibersMT IS NULL THEN 0
            ELSE with_null.netImportFibersMT
        END
    ) AS netImportFibersMT,
    (
        CASE
            WHEN with_null.netImportGoodsMT IS NULL THEN 0
            ELSE with_null.netImportGoodsMT
        END
    ) AS netImportGoodsMT,
    (
        CASE
            WHEN with_null.netImportResinMT IS NULL THEN 0
            ELSE with_null.netImportResinMT
        END
    ) AS netImportResinMT,
    (
        CASE
            WHEN with_null.netWasteTradeMT IS NULL THEN 0
            ELSE with_null.netWasteTradeMT
        END
    ) AS netWasteTradeMT
FROM
    (
        SELECT
            with_product_trade.year AS year,
            with_product_trade.region AS region,
            with_product_trade.agricultureMT AS consumptionAgricultureMT,
            with_product_trade.constructionMT AS consumptionConstructionMT,
            with_product_trade.electronicMT AS consumptionElectronicMT,
            with_product_trade.householdLeisureSportsMT AS consumptionHouseholdLeisureSportsMT,
            with_product_trade.packagingMT AS consumptionPackagingMT,
            with_product_trade.transportationMT AS consumptionTransportationMT,
            with_product_trade.textileMT AS consumptionTextileMT,
            with_product_trade.otherMT AS consumptionOtherMT,
            with_product_trade.eolRecyclingPercent AS eolRecyclingPercent,
            with_product_trade.eolIncinerationPercent AS eolIncinerationPercent,
            with_product_trade.eolLandfillPercent AS eolLandfillPercent,
            with_product_trade.eolMismanagedPercent AS eolMismanagedPercent,
            with_product_trade.netImportArticlesMT AS netImportArticlesMT,
            with_product_trade.netImportFibersMT AS netImportFibersMT,
            with_product_trade.netImportGoodsMT AS netImportGoodsMT,
            with_product_trade.netImportResinMT AS netImportResinMT,
            net_waste_trade.netMT AS netWasteTradeMT
        FROM
            (
                SELECT
                    with_eol.year AS year,
                    with_eol.region AS region,
                    with_eol.agricultureMT AS agricultureMT,
                    with_eol.constructionMT AS constructionMT,
                    with_eol.electronicMT AS electronicMT,
                    with_eol.householdLeisureSportsMT AS householdLeisureSportsMT,
                    with_eol.packagingMT AS packagingMT,
                    with_eol.transportationMT AS transportationMT,
                    with_eol.textileMT AS textileMT,
                    with_eol.otherMT AS otherMT,
                    with_eol.eolRecyclingPercent AS eolRecyclingPercent,
                    with_eol.eolIncinerationPercent AS eolIncinerationPercent,
                    with_eol.eolLandfillPercent AS eolLandfillPercent,
                    with_eol.eolMismanagedPercent AS eolMismanagedPercent,
                    net_imports_reorg.netImportArticlesMT AS netImportArticlesMT,
                    net_imports_reorg.netImportFibersMT AS netImportFibersMT,
                    net_imports_reorg.netImportGoodsMT AS netImportGoodsMT,
                    net_imports_reorg.netImportResinMT AS netImportResinMT
                FROM
                    (
                        SELECT
                            consumption_reorg.year AS year,
                            consumption_reorg.region AS region,
                            consumption_reorg.agricultureMT AS agricultureMT,
                            consumption_reorg.constructionMT AS constructionMT,
                            consumption_reorg.electronicMT AS electronicMT,
                            consumption_reorg.householdLeisureSportsMT AS householdLeisureSportsMT,
                            consumption_reorg.packagingMT AS packagingMT,
                            consumption_reorg.transportationMT AS transportationMT,
                            consumption_reorg.textileMT AS textileMT,
                            consumption_reorg.otherMT AS otherMT,
                            eol_reorg.eolRecyclingPercent AS eolRecyclingPercent,
                            eol_reorg.eolIncinerationPercent AS eolIncinerationPercent,
                            eol_reorg.eolLandfillPercent AS eolLandfillPercent,
                            eol_reorg.eolMismanagedPercent AS eolMismanagedPercent
                        FROM
                            ( 
                                SELECT
                                    year,
                                    region,
                                    sum(
                                        CASE
                                            WHEN majorMarketSector = 'Agriculture' THEN consumptionMT
                                            ELSE 0
                                        END
                                    ) AS agricultureMT,
                                    sum(
                                        CASE
                                            WHEN majorMarketSector = 'Building_Construction' THEN consumptionMT
                                            ELSE 0
                                        END
                                    ) AS constructionMT,
                                    sum(
                                        CASE
                                            WHEN majorMarketSector = 'Electrical_Electronic' THEN consumptionMT
                                            ELSE 0
                                        END
                                    ) AS electronicMT,
                                    sum(
                                        CASE
                                            WHEN majorMarketSector = 'Household_Leisure_Sports' THEN consumptionMT
                                            ELSE 0
                                        END
                                    ) AS householdLeisureSportsMT,
                                    sum(
                                        CASE
                                            WHEN majorMarketSector = 'Packaging' THEN consumptionMT
                                            ELSE 0
                                        END
                                    ) AS packagingMT,
                                    sum(
                                        CASE
                                            WHEN majorMarketSector = 'Transportation' THEN consumptionMT
                                            ELSE 0
                                        END
                                    ) AS transportationMT,
                                    sum(
                                        CASE
                                            WHEN majorMarketSector = 'Textile' THEN consumptionMT
                                            ELSE 0
                                        END
                                    ) AS textileMT,
                                    sum(
                                        CASE
                                            WHEN majorMarketSector = 'Others' THEN consumptionMT
                                            ELSE 0
                                        END
                                    ) AS otherMT
                                FROM
                                    consumption_primary
                                GROUP BY
                                    year,
                                    region
                            ) consumption_reorg
                        INNER JOIN
                            (
                                SELECT
                                    year,
                                    region,
                                    SUM(
                                        CASE
                                            WHEN type = 'recycling' THEN percent
                                            ELSE 0
                                        END
                                    ) AS eolRecyclingPercent,
                                    SUM(
                                        CASE
                                            WHEN type = 'incineration' THEN percent
                                            ELSE 0
                                        END
                                    ) AS eolIncinerationPercent,
                                    SUM(
                                        CASE
                                            WHEN type = 'landfill' THEN percent
                                            ELSE 0
                                        END
                                    ) AS eolLandfillPercent,
                                    SUM(
                                        CASE
                                            WHEN type = 'mismanaged' THEN percent
                                            ELSE 0
                                        END
                                    ) AS eolMismanagedPercent
                                FROM
                                    eol
                                GROUP BY
                                    year,
                                    region
                            ) eol_reorg
                        ON
                            consumption_reorg.year = eol_reorg.year
                            AND consumption_reorg.region = eol_reorg.region
                    ) with_eol
                LEFT OUTER JOIN
                    (
                        SELECT
                            year,
                            region,
                            SUM(
                                CASE
                                    WHEN type = 'articles' THEN netMT
                                    ELSE 0
                                END
                            ) AS netImportArticlesMT,
                            SUM(
                                CASE
                                    WHEN type = 'fibers' THEN netMT
                                    ELSE 0
                                END
                            ) AS netImportFibersMT,
                            SUM(
                                CASE
                                    WHEN type = 'goods' THEN netMT
                                    ELSE 0
                                END
                            ) AS netImportGoodsMT,
                            SUM(
                                CASE
                                    WHEN type = 'resin' THEN netMT
                                    ELSE 0
                                END
                            ) AS netImportResinMT
                        FROM
                            net_imports
                        GROUP BY
                            year,
                            region
                    ) net_imports_reorg
                ON
                    with_eol.year = net_imports_reorg.year
                    AND with_eol.region = net_imports_reorg.region
            ) with_product_trade
        LEFT OUTER JOIN
            net_waste_trade
        ON
            with_product_trade.year = net_waste_trade.year
            AND with_product_trade.region = net_waste_trade.region
    ) with_null
