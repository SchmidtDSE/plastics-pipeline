CREATE VIEW consumption_totals AS
SELECT
    year,
    region,
    majorMarketSector,
    sum(totalConsumptionMT) / count(1) AS consumptionMT
FROM
    consumption_raw
GROUP BY
    year,
    region,
    majorMarketSector;


CREATE VIEW consumption_overview AS
SELECT
    year,
    region,
    sum(
        CASE
            WHEN majorMarketSector = 'agriculture' THEN consumptionMT
            ELSE 0
        END
    ) / sum(
        CASE
            WHEN majorMarketSector = 'agriculture' THEN 1
            ELSE 0
        END
    ) AS consumptionAgricultureMT,
    sum(
        CASE
            WHEN majorMarketSector = 'building_construction' THEN consumptionMT
            ELSE 0
        END
    ) / sum(
        CASE
            WHEN majorMarketSector = 'building_construction' THEN 1
            ELSE 0
        END
    ) AS consumptionConstructionMT,
    sum(
        CASE
            WHEN majorMarketSector = 'electrical_electronic' THEN consumptionMT
            ELSE 0
        END
    ) AS consumptionElectronicMT,
    sum(
        CASE
            WHEN majorMarketSector = 'household_leisure_sports' THEN consumptionMT
            ELSE 0
        END
    ) / sum(
        CASE
            WHEN majorMarketSector = 'household_leisure_sports' THEN 1
            ELSE 0
        END
    ) AS consumptionHouseholdLeisureSportsMT,
    sum(
        CASE
            WHEN majorMarketSector = 'packaging' THEN consumptionMT
            ELSE 0
        END
    ) / sum(
        CASE
            WHEN majorMarketSector = 'packaging' THEN 1
            ELSE 0
        END
    ) AS consumptionPackagingMT,
    sum(
        CASE
            WHEN majorMarketSector = 'transportation' THEN consumptionMT
            ELSE 0
        END
    ) / sum(
        CASE
            WHEN majorMarketSector = 'transportation' THEN 1
            ELSE 0
        END
    ) AS consumptionTransporationMT,
    sum(
        CASE
            WHEN majorMarketSector = 'textile' THEN consumptionMT
            ELSE 0
        END
    ) / sum(
        CASE
            WHEN majorMarketSector = 'textile' THEN 1
            ELSE 0
        END
    ) AS consumptionTextileMT,
    sum(
        CASE
            WHEN majorMarketSector = 'others' THEN consumptionMT
            ELSE 0
        END
    ) / sum(
        CASE
            WHEN majorMarketSector = 'others' THEN 1
            ELSE 0
        END
    ) AS consumptionOtherMT
FROM
    consumption_raw_pre_total
WHERE
    year >= 2000
GROUP BY
    year,
    region;

CREATE VIEW eol_overview AS
SELECT
    year,
    region,
    sum(
        CASE
            WHEN eol = 'recycling' THEN eolMT
            ELSE 0
        END
    ) / sum(
        CASE
            WHEN eol = 'recycling' THEN 1
            ELSE 0
        END
    ) AS eolRecyclingMT,
    sum(
        CASE
            WHEN eol = 'landfill' THEN eolMT
            ELSE 0
        END
    ) / sum(
        CASE
            WHEN eol = 'landfill' THEN 1
            ELSE 0
        END
    ) AS eolLandfillMT,
    sum(
        CASE
            WHEN eol = 'incineration' THEN eolMT
            ELSE 0
        END
    ) / sum(
        CASE
            WHEN eol = 'incineration' THEN 1
            ELSE 0
        END
    ) AS eolIncinerationMT,
    sum(
        CASE
            WHEN eol = 'mismanaged' THEN eolMT
            ELSE 0
        END
    ) / sum(
        CASE
            WHEN eol = 'mismanaged' THEN 1
            ELSE 0
        END
    ) AS eolMismanagedMT
FROM
    eol_raw
WHERE
    year >= 2000
GROUP BY
    year,
    region;

CREATE VIEW net_imports_percent AS
SELECT
    total_consumption.region,
    total_consumption.year,
    net_imports_abs.netImports / total_consumption.totalConsumptionMT AS percentNetImports
FROM
    (
        SELECT
            region,
            year AS year,
            sum(consumptionMT) AS totalConsumptionMT
        FROM
            consumption_raw_pre_total
        GROUP BY
            region,
            year
    ) total_consumption
INNER JOIN
    net_imports_abs
ON
    total_consumption.region = net_imports_abs.region
    AND total_consumption.year = net_imports_abs.year;
