CREATE ROLE leo NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT LOGIN NOREPLICATION NOBYPASSRLS PASSWORD 'bbv7of1v44';
GRANT SELECT ON TABLE public.label_tillage TO leo;
GRANT SELECT ON TABLE public.label_vpd TO leo;
GRANT SELECT ON TABLE public.raw_location TO leo;
GRANT SELECT ON TABLE public.raw_rci TO leo;
GRANT SELECT ON TABLE public.raw_tillage TO leo;
GRANT SELECT ON TABLE public.join_preview TO leo;
GRANT SELECT ON TABLE public.transform_rci TO leo;
GRANT SELECT ON TABLE public.transform_tillage TO leo;CREATE VIEW consumption_raw AS
SELECT
    year AS year,
    region AS region,
    (
        CASE
            WHEN majorMarketSector = 'textiles' THEN 'textile'
            ELSE majorMarketSector
        END
    ) AS majorMarketSector,
    totalConsumptionMT AS totalConsumptionMT,
    polymer AS polymer,
    polymerPercentage AS polymerPercentage,
    polymerWaste AS polymerWaste
FROM
    (
        SELECT
            CAST(YEAR_2 AS INTEGER) AS year,
            lower(Region) AS region,
            lower(Major_Market_Sector) AS majorMarketSector,
            CAST(WasteGen AS REAL) AS totalConsumptionMT,
            lower(Type) AS polymer,
            CAST(Percentage AS REAL) AS polymerPercentage,
            CAST("WastGen_2" AS REAL) AS polymerWaste
        FROM
            file_waste_gen
    );


CREATE VIEW consumption_raw_pre_total AS
SELECT
    summarized.year AS year,
    summarized.region AS region,
    (
        CASE
            WHEN summarized.majorMarketSector = 'textiles' THEN 'textile'
            ELSE summarized.majorMarketSector
        END
    ) AS majorMarketSector,
    summarized.consumptionMT AS consumptionMT
FROM
    (
        SELECT
            interpreted.year AS year,
            interpreted.region AS region,
            interpreted.majorMarketSector AS majorMarketSector,
            sum(consumptionMT) AS consumptionMT
        FROM
            (
                SELECT
                    CAST(YEAR AS INTEGER) AS year,
                    lower(Region) AS region,
                    lower(Major_Market_Sector) AS majorMarketSector,
                    CAST(PC_Polymer AS REAL) AS consumptionMT
                FROM
                    file_consumption
            ) interpreted
        GROUP BY
            interpreted.year,
            interpreted.region,
            interpreted.majorMarketSector
    ) summarized ;

CREATE VIEW eol_raw AS
SELECT
    CAST(YEAR_2 AS INTEGER) AS year,
    lower(Region) AS region,
    lower(EOL) AS eol,
    CAST(Real_EOL AS REAL) AS eolMT
FROM
    file_eol;

CREATE VIEW net_imports_abs AS
SELECT
    (
        CAST(Transportation AS REAL) +
        CAST(Packaging AS REAL) +
        CAST(Building_Construction AS REAL) +
        CAST(Electrical_Electronic AS REAL) + 
        CAST(Household_Leisure_Sports AS REAL) +
        CAST(Agriculture AS REAL) +
        CAST(Textiles AS REAL) +
        CAST(Others AS REAL)
    ) AS netImports,
    CAST(Year AS INTEGER) AS year,
    'china' AS region
FROM
    file_18_net_trade_china
UNION ALL
SELECT
    (
        CAST(Transportation AS REAL) +
        CAST(Packaging AS REAL) +
        CAST(Building_Construction AS REAL) +
        CAST(Electrical_Electronic AS REAL) + 
        CAST(Household_Leisure_Sports AS REAL) +
        CAST(Agriculture AS REAL) +
        CAST(Textiles AS REAL) +
        CAST(Others AS REAL)
    ) AS netImports,
    CAST(Year AS INTEGER) AS year,
    'nafta' AS region
FROM
    file_19_net_trade_nafta
UNION ALL
SELECT
    (
        CAST(Transportation AS REAL) +
        CAST(Packaging AS REAL) +
        CAST(Building_Construction AS REAL) +
        CAST(Electrical_Electronic AS REAL) + 
        CAST(Household_Leisure_Sports AS REAL) +
        CAST(Agriculture AS REAL) +
        CAST(Textiles AS REAL) +
        CAST(Others AS REAL)
    ) AS netImports,
    CAST(Year AS INTEGER) AS year,
    'eu30' AS region
FROM
    file_20_net_trade_eu
UNION ALL
SELECT
    (
        CAST(Transportation AS REAL) +
        CAST(Packaging AS REAL) +
        CAST(Building_Construction AS REAL) +
        CAST(Electrical_Electronic AS REAL) + 
        CAST(Household_Leisure_Sports AS REAL) +
        CAST(Agriculture AS REAL) +
        CAST(Textiles AS REAL) +
        CAST(Others AS REAL)
    ) AS netImports,
    CAST(Year AS INTEGER) AS year,
    'row' AS region
FROM
    file_21_net_trade_row
