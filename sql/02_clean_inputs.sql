CREATE VIEW consumption_raw AS
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
    year AS year,
    region AS region,
    (
        CASE
            WHEN majorMarketSector = 'textiles' THEN 'textile'
            ELSE majorMarketSector
        END
    ) AS majorMarketSector,
    consumptionMT AS consumptionMT
FROM
    (
        SELECT
            CAST(YEAR AS INTEGER) AS year,
            lower(Region) AS region,
            lower(Major_Market_Sector) AS majorMarketSector,
            CAST(PC_Sector AS REAL) AS consumptionMT
        FROM
            file_consumption
    );

CREATE VIEW eol_raw AS
SELECT
    CAST(YEAR_2 AS INTEGER) AS year,
    lower(Region) AS region,
    lower(EOL) AS eol,
    CAST(Prim_W_EOL AS REAL) AS eolMT
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
