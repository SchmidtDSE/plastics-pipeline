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
            file_kd2
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
            file_ka3
    );

CREATE VIEW eol_raw AS
SELECT
    CAST(YEAR_2 AS INTEGER) AS year,
    lower(Region) AS region,
    lower(EOL) AS eol,
    CAST(EOL_Waste_5 AS REAL) AS eolMT
FROM
    file_ma6;
