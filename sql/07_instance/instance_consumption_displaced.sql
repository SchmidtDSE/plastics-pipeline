CREATE VIEW instance_consumption_displaced AS
SELECT
    years,
    popChange,
    gdpChange,
    afterGdp,
    afterPopulation,
    (
        CASE
            WHEN unioned.region = 'china' THEN 1
            ELSE 0
        END
    ) AS flagChina,
    (
        CASE
            WHEN unioned.region = 'eu30' THEN 1
            ELSE 0
        END
    ) AS flagEU30,
    (
        CASE
            WHEN unioned.region = 'nafta' THEN 1
            ELSE 0
        END
    ) AS flagNafta,
    (
        CASE
            WHEN unioned.region = 'row' THEN 1
            ELSE 0
        END
    ) AS flagRow,
    (
        CASE
            WHEN unioned.majorMarketSector = 'Agriculture' THEN 1
            ELSE 0
        END
    ) AS flagAgriculture,
    (
        CASE
            WHEN unioned.majorMarketSector = 'Building_Construction' THEN 1
            ELSE 0
        END
    ) AS flagConstruction,
    (
        CASE
            WHEN unioned.majorMarketSector = 'Electrical_Electronic' THEN 1
            ELSE 0
        END
    ) AS flagElectronic,
    (
        CASE
            WHEN unioned.majorMarketSector = 'Household_Leisure_Sports' THEN 1
            ELSE 0
        END
    ) AS flagHouseholdLeisureSports,
    (
        CASE
            WHEN unioned.majorMarketSector = 'Others' THEN 1
            ELSE 0
        END
    ) AS flagOther,
    (
        CASE
            WHEN unioned.majorMarketSector = 'Packaging' THEN 1
            ELSE 0
        END
    ) AS flagPackaging,
    (
        CASE
            WHEN unioned.majorMarketSector = 'Transportation' THEN 1
            ELSE 0
        END
    ) AS flagTextile,
    (
        CASE
            WHEN unioned.majorMarketSector = 'Textile' THEN 1
            ELSE 0
        END
    ) AS flagTransporation,
    unioned.consumptionChange AS consumptionChange,
    unioned.beforeConsumptionMT AS beforeConsumptionMT,
    unioned.afterConsumptionMT AS afterConsumptionMT,
    unioned.beforeYear AS beforeYear,
    unioned.afterYear AS afterYear
FROM
    (
        SELECT
            abs(after.afterYear - before.beforeYear) AS years,
            (afterPopulation - beforePopulation) / beforePopulation AS popChange,
            (afterGdp - beforeGdp) / beforeGdp AS gdpChange,
            afterGdp AS afterGdp,
            afterPopulation AS afterPopulation,
            (afterConsumptionMT - beforeConsumptionMT) / beforeConsumptionMT AS consumptionChange,
            after.region AS region,
            after.majorMarketSector AS majorMarketSector,
            before.beforeConsumptionMT AS beforeConsumptionMT,
            after.afterConsumptionMT AS afterConsumptionMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear
        FROM
            (
                SELECT
                    instance_consumption_normal.year AS afterYear,
                    instance_consumption_normal.region AS region,
                    instance_consumption_normal.majorMarketSector AS majorMarketSector,
                    instance_consumption_normal.population AS afterPopulation,
                    instance_consumption_normal.gdp AS afterGdp,
                    instance_consumption_normal.consumptionMT AS afterConsumptionMT
                FROM
                    instance_consumption_normal
            ) after
        INNER JOIN
            (
                SELECT
                    instance_consumption_normal.year AS beforeYear,
                    instance_consumption_normal.region AS region,
                    instance_consumption_normal.majorMarketSector AS majorMarketSector,
                    instance_consumption_normal.population AS beforePopulation,
                    instance_consumption_normal.gdp AS beforeGdp,
                    instance_consumption_normal.consumptionMT AS beforeConsumptionMT
                FROM
                    instance_consumption_normal
            ) before
        ON
            after.afterYear = before.beforeYear + 1
            AND after.region = before.region
            AND after.majorMarketSector = before.majorMarketSector
        UNION ALL
        SELECT
            abs(after.afterYear - before.beforeYear) AS years,
            (afterPopulation - beforePopulation) / beforePopulation AS popChange,
            (afterGdp - beforeGdp) / beforeGdp AS gdpChange,
            afterGdp AS afterGdp,
            afterPopulation AS afterPopulation,
            (afterConsumptionMT - beforeConsumptionMT) / beforeConsumptionMT AS consumptionChange,
            after.region AS region,
            after.majorMarketSector AS majorMarketSector,
            before.beforeConsumptionMT AS beforeConsumptionMT,
            after.afterConsumptionMT AS afterConsumptionMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear
        FROM
            (
                SELECT
                    instance_consumption_normal.year AS afterYear,
                    instance_consumption_normal.region AS region,
                    instance_consumption_normal.majorMarketSector AS majorMarketSector,
                    instance_consumption_normal.population AS afterPopulation,
                    instance_consumption_normal.gdp AS afterGdp,
                    instance_consumption_normal.consumptionMT AS afterConsumptionMT
                FROM
                    instance_consumption_normal
            ) after
        INNER JOIN
            (
                SELECT
                    instance_consumption_normal.year AS beforeYear,
                    instance_consumption_normal.region AS region,
                    instance_consumption_normal.majorMarketSector AS majorMarketSector,
                    instance_consumption_normal.population AS beforePopulation,
                    instance_consumption_normal.gdp AS beforeGdp,
                    instance_consumption_normal.consumptionMT AS beforeConsumptionMT
                FROM
                    instance_consumption_normal
            ) before
        ON
            after.afterYear = before.beforeYear + 2
            AND after.region = before.region
            AND after.majorMarketSector = before.majorMarketSector
        UNION ALL
        SELECT
            abs(after.afterYear - before.beforeYear) AS years,
            (afterPopulation - beforePopulation) / beforePopulation AS popChange,
            (afterGdp - beforeGdp) / beforeGdp AS gdpChange,
            afterGdp AS afterGdp,
            afterPopulation AS afterPopulation,
            (afterConsumptionMT - beforeConsumptionMT) / beforeConsumptionMT AS consumptionChange,
            after.region AS region,
            after.majorMarketSector AS majorMarketSector,
            before.beforeConsumptionMT AS beforeConsumptionMT,
            after.afterConsumptionMT AS afterConsumptionMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear
        FROM
            (
                SELECT
                    instance_consumption_normal.year AS afterYear,
                    instance_consumption_normal.region AS region,
                    instance_consumption_normal.majorMarketSector AS majorMarketSector,
                    instance_consumption_normal.population AS afterPopulation,
                    instance_consumption_normal.gdp AS afterGdp,
                    instance_consumption_normal.consumptionMT AS afterConsumptionMT
                FROM
                    instance_consumption_normal
            ) after
        INNER JOIN
            (
                SELECT
                    instance_consumption_normal.year AS beforeYear,
                    instance_consumption_normal.region AS region,
                    instance_consumption_normal.majorMarketSector AS majorMarketSector,
                    instance_consumption_normal.population AS beforePopulation,
                    instance_consumption_normal.gdp AS beforeGdp,
                    instance_consumption_normal.consumptionMT AS beforeConsumptionMT
                FROM
                    instance_consumption_normal
            ) before
        ON
            after.afterYear = before.beforeYear + 3
            AND after.region = before.region
            AND after.majorMarketSector = before.majorMarketSector
        UNION ALL
        SELECT
            abs(after.afterYear - before.beforeYear) AS years,
            (afterPopulation - beforePopulation) / beforePopulation AS popChange,
            (afterGdp - beforeGdp) / beforeGdp AS gdpChange,
            afterGdp AS afterGdp,
            afterPopulation AS afterPopulation,
            (afterConsumptionMT - beforeConsumptionMT) / beforeConsumptionMT AS consumptionChange,
            after.region AS region,
            after.majorMarketSector AS majorMarketSector,
            before.beforeConsumptionMT AS beforeConsumptionMT,
            after.afterConsumptionMT AS afterConsumptionMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear
        FROM
            (
                SELECT
                    instance_consumption_normal.year AS afterYear,
                    instance_consumption_normal.region AS region,
                    instance_consumption_normal.majorMarketSector AS majorMarketSector,
                    instance_consumption_normal.population AS afterPopulation,
                    instance_consumption_normal.gdp AS afterGdp,
                    instance_consumption_normal.consumptionMT AS afterConsumptionMT
                FROM
                    instance_consumption_normal
            ) after
        INNER JOIN
            (
                SELECT
                    instance_consumption_normal.year AS beforeYear,
                    instance_consumption_normal.region AS region,
                    instance_consumption_normal.majorMarketSector AS majorMarketSector,
                    instance_consumption_normal.population AS beforePopulation,
                    instance_consumption_normal.gdp AS beforeGdp,
                    instance_consumption_normal.consumptionMT AS beforeConsumptionMT
                FROM
                    instance_consumption_normal
            ) before
        ON
            after.afterYear = before.beforeYear + 4
            AND after.region = before.region
            AND after.majorMarketSector = before.majorMarketSector
        UNION ALL
        SELECT
            abs(after.afterYear - before.beforeYear) AS years,
            (afterPopulation - beforePopulation) / beforePopulation AS popChange,
            (afterGdp - beforeGdp) / beforeGdp AS gdpChange,
            afterGdp AS afterGdp,
            afterPopulation AS afterPopulation,
            (afterConsumptionMT - beforeConsumptionMT) / beforeConsumptionMT AS consumptionChange,
            after.region AS region,
            after.majorMarketSector AS majorMarketSector,
            before.beforeConsumptionMT AS beforeConsumptionMT,
            after.afterConsumptionMT AS afterConsumptionMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear
        FROM
            (
                SELECT
                    instance_consumption_normal.year AS afterYear,
                    instance_consumption_normal.region AS region,
                    instance_consumption_normal.majorMarketSector AS majorMarketSector,
                    instance_consumption_normal.population AS afterPopulation,
                    instance_consumption_normal.gdp AS afterGdp,
                    instance_consumption_normal.consumptionMT AS afterConsumptionMT
                FROM
                    instance_consumption_normal
            ) after
        INNER JOIN
            (
                SELECT
                    instance_consumption_normal.year AS beforeYear,
                    instance_consumption_normal.region AS region,
                    instance_consumption_normal.majorMarketSector AS majorMarketSector,
                    instance_consumption_normal.population AS beforePopulation,
                    instance_consumption_normal.gdp AS beforeGdp,
                    instance_consumption_normal.consumptionMT AS beforeConsumptionMT
                FROM
                    instance_consumption_normal
            ) before
        ON
            after.afterYear = before.beforeYear + 5
            AND after.region = before.region
            AND after.majorMarketSector = before.majorMarketSector
        UNION ALL
        SELECT
            abs(after.afterYear - before.beforeYear) AS years,
            (afterPopulation - beforePopulation) / beforePopulation AS popChange,
            (afterGdp - beforeGdp) / beforeGdp AS gdpChange,
            afterGdp AS afterGdp,
            afterPopulation AS afterPopulation,
            (afterConsumptionMT - beforeConsumptionMT) / beforeConsumptionMT AS consumptionChange,
            after.region AS region,
            after.majorMarketSector AS majorMarketSector,
            before.beforeConsumptionMT AS beforeConsumptionMT,
            after.afterConsumptionMT AS afterConsumptionMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear
        FROM
            (
                SELECT
                    instance_consumption_normal.year AS afterYear,
                    instance_consumption_normal.region AS region,
                    instance_consumption_normal.majorMarketSector AS majorMarketSector,
                    instance_consumption_normal.population AS afterPopulation,
                    instance_consumption_normal.gdp AS afterGdp,
                    instance_consumption_normal.consumptionMT AS afterConsumptionMT
                FROM
                    instance_consumption_normal
            ) after
        INNER JOIN
            (
                SELECT
                    instance_consumption_normal.year AS beforeYear,
                    instance_consumption_normal.region AS region,
                    instance_consumption_normal.majorMarketSector AS majorMarketSector,
                    instance_consumption_normal.population AS beforePopulation,
                    instance_consumption_normal.gdp AS beforeGdp,
                    instance_consumption_normal.consumptionMT AS beforeConsumptionMT
                FROM
                    instance_consumption_normal
            ) before
        ON
            after.afterYear = before.beforeYear - 1
            AND after.region = before.region
            AND after.majorMarketSector = before.majorMarketSector
        UNION ALL
        SELECT
            abs(after.afterYear - before.beforeYear) AS years,
            (afterPopulation - beforePopulation) / beforePopulation AS popChange,
            (afterGdp - beforeGdp) / beforeGdp AS gdpChange,
            afterGdp AS afterGdp,
            afterPopulation AS afterPopulation,
            (afterConsumptionMT - beforeConsumptionMT) / beforeConsumptionMT AS consumptionChange,
            after.region AS region,
            after.majorMarketSector AS majorMarketSector,
            before.beforeConsumptionMT AS beforeConsumptionMT,
            after.afterConsumptionMT AS afterConsumptionMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear
        FROM
            (
                SELECT
                    instance_consumption_normal.year AS afterYear,
                    instance_consumption_normal.region AS region,
                    instance_consumption_normal.majorMarketSector AS majorMarketSector,
                    instance_consumption_normal.population AS afterPopulation,
                    instance_consumption_normal.gdp AS afterGdp,
                    instance_consumption_normal.consumptionMT AS afterConsumptionMT
                FROM
                    instance_consumption_normal
            ) after
        INNER JOIN
            (
                SELECT
                    instance_consumption_normal.year AS beforeYear,
                    instance_consumption_normal.region AS region,
                    instance_consumption_normal.majorMarketSector AS majorMarketSector,
                    instance_consumption_normal.population AS beforePopulation,
                    instance_consumption_normal.gdp AS beforeGdp,
                    instance_consumption_normal.consumptionMT AS beforeConsumptionMT
                FROM
                    instance_consumption_normal
            ) before
        ON
            after.afterYear = before.beforeYear - 2
            AND after.region = before.region
            AND after.majorMarketSector = before.majorMarketSector
        UNION ALL
        SELECT
            abs(after.afterYear - before.beforeYear) AS years,
            (afterPopulation - beforePopulation) / beforePopulation AS popChange,
            (afterGdp - beforeGdp) / beforeGdp AS gdpChange,
            afterGdp AS afterGdp,
            afterPopulation AS afterPopulation,
            (afterConsumptionMT - beforeConsumptionMT) / beforeConsumptionMT AS consumptionChange,
            after.region AS region,
            after.majorMarketSector AS majorMarketSector,
            before.beforeConsumptionMT AS beforeConsumptionMT,
            after.afterConsumptionMT AS afterConsumptionMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear
        FROM
            (
                SELECT
                    instance_consumption_normal.year AS afterYear,
                    instance_consumption_normal.region AS region,
                    instance_consumption_normal.majorMarketSector AS majorMarketSector,
                    instance_consumption_normal.population AS afterPopulation,
                    instance_consumption_normal.gdp AS afterGdp,
                    instance_consumption_normal.consumptionMT AS afterConsumptionMT
                FROM
                    instance_consumption_normal
            ) after
        INNER JOIN
            (
                SELECT
                    instance_consumption_normal.year AS beforeYear,
                    instance_consumption_normal.region AS region,
                    instance_consumption_normal.majorMarketSector AS majorMarketSector,
                    instance_consumption_normal.population AS beforePopulation,
                    instance_consumption_normal.gdp AS beforeGdp,
                    instance_consumption_normal.consumptionMT AS beforeConsumptionMT
                FROM
                    instance_consumption_normal
            ) before
        ON
            after.afterYear = before.beforeYear - 3
            AND after.region = before.region
            AND after.majorMarketSector = before.majorMarketSector
        UNION ALL
        SELECT
            abs(after.afterYear - before.beforeYear) AS years,
            (afterPopulation - beforePopulation) / beforePopulation AS popChange,
            (afterGdp - beforeGdp) / beforeGdp AS gdpChange,
            afterGdp AS afterGdp,
            afterPopulation AS afterPopulation,
            (afterConsumptionMT - beforeConsumptionMT) / beforeConsumptionMT AS consumptionChange,
            after.region AS region,
            after.majorMarketSector AS majorMarketSector,
            before.beforeConsumptionMT AS beforeConsumptionMT,
            after.afterConsumptionMT AS afterConsumptionMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear
        FROM
            (
                SELECT
                    instance_consumption_normal.year AS afterYear,
                    instance_consumption_normal.region AS region,
                    instance_consumption_normal.majorMarketSector AS majorMarketSector,
                    instance_consumption_normal.population AS afterPopulation,
                    instance_consumption_normal.gdp AS afterGdp,
                    instance_consumption_normal.consumptionMT AS afterConsumptionMT
                FROM
                    instance_consumption_normal
            ) after
        INNER JOIN
            (
                SELECT
                    instance_consumption_normal.year AS beforeYear,
                    instance_consumption_normal.region AS region,
                    instance_consumption_normal.majorMarketSector AS majorMarketSector,
                    instance_consumption_normal.population AS beforePopulation,
                    instance_consumption_normal.gdp AS beforeGdp,
                    instance_consumption_normal.consumptionMT AS beforeConsumptionMT
                FROM
                    instance_consumption_normal
            ) before
        ON
            after.afterYear = before.beforeYear - 4
            AND after.region = before.region
            AND after.majorMarketSector = before.majorMarketSector
        UNION ALL
        SELECT
            abs(after.afterYear - before.beforeYear) AS years,
            (afterPopulation - beforePopulation) / beforePopulation AS popChange,
            (afterGdp - beforeGdp) / beforeGdp AS gdpChange,
            afterGdp AS afterGdp,
            afterPopulation AS afterPopulation,
            (afterConsumptionMT - beforeConsumptionMT) / beforeConsumptionMT AS consumptionChange,
            after.region AS region,
            after.majorMarketSector AS majorMarketSector,
            before.beforeConsumptionMT AS beforeConsumptionMT,
            after.afterConsumptionMT AS afterConsumptionMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear
        FROM
            (
                SELECT
                    instance_consumption_normal.year AS afterYear,
                    instance_consumption_normal.region AS region,
                    instance_consumption_normal.majorMarketSector AS majorMarketSector,
                    instance_consumption_normal.population AS afterPopulation,
                    instance_consumption_normal.gdp AS afterGdp,
                    instance_consumption_normal.consumptionMT AS afterConsumptionMT
                FROM
                    instance_consumption_normal
            ) after
        INNER JOIN
            (
                SELECT
                    instance_consumption_normal.year AS beforeYear,
                    instance_consumption_normal.region AS region,
                    instance_consumption_normal.majorMarketSector AS majorMarketSector,
                    instance_consumption_normal.population AS beforePopulation,
                    instance_consumption_normal.gdp AS beforeGdp,
                    instance_consumption_normal.consumptionMT AS beforeConsumptionMT
                FROM
                    instance_consumption_normal
            ) before
        ON
            after.afterYear = before.beforeYear - 5
            AND after.region = before.region
            AND after.majorMarketSector = before.majorMarketSector
    ) unioned