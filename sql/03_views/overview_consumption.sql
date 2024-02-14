CREATE VIEW overview_consumption AS
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
    consumption
GROUP BY
    year,
    region