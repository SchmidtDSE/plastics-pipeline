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
    ) AS consumptionAgricultureMT,
    sum(
        CASE
            WHEN majorMarketSector = 'building_construction' THEN consumptionMT
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
    ) AS consumptionHouseholdLeisureSportsMT,
    sum(
        CASE
            WHEN majorMarketSector = 'packaging' THEN consumptionMT
            ELSE 0
        END
    ) AS consumptionPackagingMT,
    sum(
        CASE
            WHEN majorMarketSector = 'transportation' THEN consumptionMT
            ELSE 0
        END
    ) AS consumptionTransporationMT,
    sum(
        CASE
            WHEN majorMarketSector = 'textile' THEN consumptionMT
            ELSE 0
        END
    ) AS consumptionTextitleMT,
    sum(
        CASE
            WHEN majorMarketSector = 'others' THEN consumptionMT
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
    ) AS eolRecyclingMT,
    sum(
        CASE
            WHEN eol = 'landfill' THEN eolMT
            ELSE 0
        END
    ) AS eolLandfillMT,
    sum(
        CASE
            WHEN eol = 'incineration' THEN eolMT
            ELSE 0
        END
    ) AS eolIncinerationMT,
    sum(
        CASE
            WHEN eol = 'mismanaged' THEN eolMT
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
