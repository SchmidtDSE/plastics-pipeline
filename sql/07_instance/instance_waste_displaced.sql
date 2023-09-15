CREATE VIEW instance_waste_displaced AS
SELECT
    unioned.years AS years,
    unioned.popChange AS popChange,
    unioned.gdpChange AS gdpChange,
    unioned.afterGdp AS afterGdp,
    unioned.beforePercent AS beforePercent,
    unioned.percentChange AS percentChange,
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
            WHEN unioned.type = 'recycling' THEN 1
            ELSE 0
        END
    ) AS flagRecycling,
    (
        CASE
            WHEN unioned.type = 'incineration' THEN 1
            ELSE 0
        END
    ) AS flagIncineration,
    (
        CASE
            WHEN unioned.type = 'landfill' THEN 1
            ELSE 0
        END
    ) AS flagLandfill,
    (
        CASE
            WHEN unioned.type = 'mismanaged' THEN 1
            ELSE 0
        END
    ) AS flagMismanaged,
    unioned.afterPercent AS afterPercent,
    unioned.beforeYear AS beforeYear,
    unioned.afterYear AS afterYear
FROM
    (
        SELECT
            abs(after.afterYear - before.beforeYear) AS years,
            (afterPopulation - beforePopulation) / beforePopulation AS popChange,
            (afterGdp - beforeGdp) / beforeGdp AS gdpChange,
            (afterPercent - beforePercent) / beforePercent AS percentChange,
            after.region AS region,
            after.type AS type,
            after.afterGdp AS afterGdp,
            before.beforePercent AS beforePercent,
            after.afterPercent AS afterPercent,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear
        FROM
            (
                SELECT
                    instance_waste_normal.year AS afterYear,
                    instance_waste_normal.region AS region,
                    instance_waste_normal.type AS type,
                    instance_waste_normal.population AS afterPopulation,
                    instance_waste_normal.gdp AS afterGdp,
                    instance_waste_normal.percent AS afterPercent
                FROM
                    instance_waste_normal
            ) after
        INNER JOIN
            (
                SELECT
                    instance_waste_normal.year AS beforeYear,
                    instance_waste_normal.region AS region,
                    instance_waste_normal.type AS type,
                    instance_waste_normal.population AS beforePopulation,
                    instance_waste_normal.gdp AS beforeGdp,
                    instance_waste_normal.percent AS beforePercent
                FROM
                    instance_waste_normal
            ) before
        ON
            after.afterYear = before.beforeYear + 1
            AND after.region = before.region
            AND after.type = before.type
        UNION ALL
        SELECT
            abs(after.afterYear - before.beforeYear) AS years,
            (afterPopulation - beforePopulation) / beforePopulation AS popChange,
            (afterGdp - beforeGdp) / beforeGdp AS gdpChange,
            (afterPercent - beforePercent) / beforePercent AS percentChange,
            after.region AS region,
            after.type AS type,
            after.afterGdp AS afterGdp,
            before.beforePercent AS beforePercent,
            after.afterPercent AS afterPercent,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear
        FROM
            (
                SELECT
                    instance_waste_normal.year AS afterYear,
                    instance_waste_normal.region AS region,
                    instance_waste_normal.type AS type,
                    instance_waste_normal.population AS afterPopulation,
                    instance_waste_normal.gdp AS afterGdp,
                    instance_waste_normal.percent AS afterPercent
                FROM
                    instance_waste_normal
            ) after
        INNER JOIN
            (
                SELECT
                    instance_waste_normal.year AS beforeYear,
                    instance_waste_normal.region AS region,
                    instance_waste_normal.type AS type,
                    instance_waste_normal.population AS beforePopulation,
                    instance_waste_normal.gdp AS beforeGdp,
                    instance_waste_normal.percent AS beforePercent
                FROM
                    instance_waste_normal
            ) before
        ON
            after.afterYear = before.beforeYear + 2
            AND after.region = before.region
            AND after.type = before.type
        UNION ALL
        SELECT
            abs(after.afterYear - before.beforeYear) AS years,
            (afterPopulation - beforePopulation) / beforePopulation AS popChange,
            (afterGdp - beforeGdp) / beforeGdp AS gdpChange,
            (afterPercent - beforePercent) / beforePercent AS percentChange,
            after.region AS region,
            after.type AS type,
            after.afterGdp AS afterGdp,
            before.beforePercent AS beforePercent,
            after.afterPercent AS afterPercent,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear
        FROM
            (
                SELECT
                    instance_waste_normal.year AS afterYear,
                    instance_waste_normal.region AS region,
                    instance_waste_normal.type AS type,
                    instance_waste_normal.population AS afterPopulation,
                    instance_waste_normal.gdp AS afterGdp,
                    instance_waste_normal.percent AS afterPercent
                FROM
                    instance_waste_normal
            ) after
        INNER JOIN
            (
                SELECT
                    instance_waste_normal.year AS beforeYear,
                    instance_waste_normal.region AS region,
                    instance_waste_normal.type AS type,
                    instance_waste_normal.population AS beforePopulation,
                    instance_waste_normal.gdp AS beforeGdp,
                    instance_waste_normal.percent AS beforePercent
                FROM
                    instance_waste_normal
            ) before
        ON
            after.afterYear = before.beforeYear + 3
            AND after.region = before.region
            AND after.type = before.type
        UNION ALL
        SELECT
            abs(after.afterYear - before.beforeYear) AS years,
            (afterPopulation - beforePopulation) / beforePopulation AS popChange,
            (afterGdp - beforeGdp) / beforeGdp AS gdpChange,
            (afterPercent - beforePercent) / beforePercent AS percentChange,
            after.region AS region,
            after.type AS type,
            after.afterGdp AS afterGdp,
            before.beforePercent AS beforePercent,
            after.afterPercent AS afterPercent,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear
        FROM
            (
                SELECT
                    instance_waste_normal.year AS afterYear,
                    instance_waste_normal.region AS region,
                    instance_waste_normal.type AS type,
                    instance_waste_normal.population AS afterPopulation,
                    instance_waste_normal.gdp AS afterGdp,
                    instance_waste_normal.percent AS afterPercent
                FROM
                    instance_waste_normal
            ) after
        INNER JOIN
            (
                SELECT
                    instance_waste_normal.year AS beforeYear,
                    instance_waste_normal.region AS region,
                    instance_waste_normal.type AS type,
                    instance_waste_normal.population AS beforePopulation,
                    instance_waste_normal.gdp AS beforeGdp,
                    instance_waste_normal.percent AS beforePercent
                FROM
                    instance_waste_normal
            ) before
        ON
            after.afterYear = before.beforeYear + 4
            AND after.region = before.region
            AND after.type = before.type
        UNION ALL
        SELECT
            abs(after.afterYear - before.beforeYear) AS years,
            (afterPopulation - beforePopulation) / beforePopulation AS popChange,
            (afterGdp - beforeGdp) / beforeGdp AS gdpChange,
            (afterPercent - beforePercent) / beforePercent AS percentChange,
            after.region AS region,
            after.type AS type,
            after.afterGdp AS afterGdp,
            before.beforePercent AS beforePercent,
            after.afterPercent AS afterPercent,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear
        FROM
            (
                SELECT
                    instance_waste_normal.year AS afterYear,
                    instance_waste_normal.region AS region,
                    instance_waste_normal.type AS type,
                    instance_waste_normal.population AS afterPopulation,
                    instance_waste_normal.gdp AS afterGdp,
                    instance_waste_normal.percent AS afterPercent
                FROM
                    instance_waste_normal
            ) after
        INNER JOIN
            (
                SELECT
                    instance_waste_normal.year AS beforeYear,
                    instance_waste_normal.region AS region,
                    instance_waste_normal.type AS type,
                    instance_waste_normal.population AS beforePopulation,
                    instance_waste_normal.gdp AS beforeGdp,
                    instance_waste_normal.percent AS beforePercent
                FROM
                    instance_waste_normal
            ) before
        ON
            after.afterYear = before.beforeYear + 5
            AND after.region = before.region
            AND after.type = before.type
        UNION ALL
        SELECT
            abs(after.afterYear - before.beforeYear) AS years,
            (afterPopulation - beforePopulation) / beforePopulation AS popChange,
            (afterGdp - beforeGdp) / beforeGdp AS gdpChange,
            (afterPercent - beforePercent) / beforePercent AS percentChange,
            after.region AS region,
            after.type AS type,
            after.afterGdp AS afterGdp,
            before.beforePercent AS beforePercent,
            after.afterPercent AS afterPercent,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear
        FROM
            (
                SELECT
                    instance_waste_normal.year AS afterYear,
                    instance_waste_normal.region AS region,
                    instance_waste_normal.type AS type,
                    instance_waste_normal.population AS afterPopulation,
                    instance_waste_normal.gdp AS afterGdp,
                    instance_waste_normal.percent AS afterPercent
                FROM
                    instance_waste_normal
            ) after
        INNER JOIN
            (
                SELECT
                    instance_waste_normal.year AS beforeYear,
                    instance_waste_normal.region AS region,
                    instance_waste_normal.type AS type,
                    instance_waste_normal.population AS beforePopulation,
                    instance_waste_normal.gdp AS beforeGdp,
                    instance_waste_normal.percent AS beforePercent
                FROM
                    instance_waste_normal
            ) before
        ON
            after.afterYear = before.beforeYear - 1
            AND after.region = before.region
            AND after.type = before.type
        UNION ALL
        SELECT
            abs(after.afterYear - before.beforeYear) AS years,
            (afterPopulation - beforePopulation) / beforePopulation AS popChange,
            (afterGdp - beforeGdp) / beforeGdp AS gdpChange,
            (afterPercent - beforePercent) / beforePercent AS percentChange,
            after.region AS region,
            after.type AS type,
            after.afterGdp AS afterGdp,
            before.beforePercent AS beforePercent,
            after.afterPercent AS afterPercent,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear
        FROM
            (
                SELECT
                    instance_waste_normal.year AS afterYear,
                    instance_waste_normal.region AS region,
                    instance_waste_normal.type AS type,
                    instance_waste_normal.population AS afterPopulation,
                    instance_waste_normal.gdp AS afterGdp,
                    instance_waste_normal.percent AS afterPercent
                FROM
                    instance_waste_normal
            ) after
        INNER JOIN
            (
                SELECT
                    instance_waste_normal.year AS beforeYear,
                    instance_waste_normal.region AS region,
                    instance_waste_normal.type AS type,
                    instance_waste_normal.population AS beforePopulation,
                    instance_waste_normal.gdp AS beforeGdp,
                    instance_waste_normal.percent AS beforePercent
                FROM
                    instance_waste_normal
            ) before
        ON
            after.afterYear = before.beforeYear - 2
            AND after.region = before.region
            AND after.type = before.type
        UNION ALL
        SELECT
            abs(after.afterYear - before.beforeYear) AS years,
            (afterPopulation - beforePopulation) / beforePopulation AS popChange,
            (afterGdp - beforeGdp) / beforeGdp AS gdpChange,
            (afterPercent - beforePercent) / beforePercent AS percentChange,
            after.region AS region,
            after.type AS type,
            after.afterGdp AS afterGdp,
            before.beforePercent AS beforePercent,
            after.afterPercent AS afterPercent,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear
        FROM
            (
                SELECT
                    instance_waste_normal.year AS afterYear,
                    instance_waste_normal.region AS region,
                    instance_waste_normal.type AS type,
                    instance_waste_normal.population AS afterPopulation,
                    instance_waste_normal.gdp AS afterGdp,
                    instance_waste_normal.percent AS afterPercent
                FROM
                    instance_waste_normal
            ) after
        INNER JOIN
            (
                SELECT
                    instance_waste_normal.year AS beforeYear,
                    instance_waste_normal.region AS region,
                    instance_waste_normal.type AS type,
                    instance_waste_normal.population AS beforePopulation,
                    instance_waste_normal.gdp AS beforeGdp,
                    instance_waste_normal.percent AS beforePercent
                FROM
                    instance_waste_normal
            ) before
        ON
            after.afterYear = before.beforeYear - 3
            AND after.region = before.region
            AND after.type = before.type
        UNION ALL
        SELECT
            abs(after.afterYear - before.beforeYear) AS years,
            (afterPopulation - beforePopulation) / beforePopulation AS popChange,
            (afterGdp - beforeGdp) / beforeGdp AS gdpChange,
            (afterPercent - beforePercent) / beforePercent AS percentChange,
            after.region AS region,
            after.type AS type,
            after.afterGdp AS afterGdp,
            before.beforePercent AS beforePercent,
            after.afterPercent AS afterPercent,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear
        FROM
            (
                SELECT
                    instance_waste_normal.year AS afterYear,
                    instance_waste_normal.region AS region,
                    instance_waste_normal.type AS type,
                    instance_waste_normal.population AS afterPopulation,
                    instance_waste_normal.gdp AS afterGdp,
                    instance_waste_normal.percent AS afterPercent
                FROM
                    instance_waste_normal
            ) after
        INNER JOIN
            (
                SELECT
                    instance_waste_normal.year AS beforeYear,
                    instance_waste_normal.region AS region,
                    instance_waste_normal.type AS type,
                    instance_waste_normal.population AS beforePopulation,
                    instance_waste_normal.gdp AS beforeGdp,
                    instance_waste_normal.percent AS beforePercent
                FROM
                    instance_waste_normal
            ) before
        ON
            after.afterYear = before.beforeYear - 4
            AND after.region = before.region
            AND after.type = before.type
        UNION ALL
        SELECT
            abs(after.afterYear - before.beforeYear) AS years,
            (afterPopulation - beforePopulation) / beforePopulation AS popChange,
            (afterGdp - beforeGdp) / beforeGdp AS gdpChange,
            (afterPercent - beforePercent) / beforePercent AS percentChange,
            after.region AS region,
            after.type AS type,
            after.afterGdp AS afterGdp,
            before.beforePercent AS beforePercent,
            after.afterPercent AS afterPercent,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear
        FROM
            (
                SELECT
                    instance_waste_normal.year AS afterYear,
                    instance_waste_normal.region AS region,
                    instance_waste_normal.type AS type,
                    instance_waste_normal.population AS afterPopulation,
                    instance_waste_normal.gdp AS afterGdp,
                    instance_waste_normal.percent AS afterPercent
                FROM
                    instance_waste_normal
            ) after
        INNER JOIN
            (
                SELECT
                    instance_waste_normal.year AS beforeYear,
                    instance_waste_normal.region AS region,
                    instance_waste_normal.type AS type,
                    instance_waste_normal.population AS beforePopulation,
                    instance_waste_normal.gdp AS beforeGdp,
                    instance_waste_normal.percent AS beforePercent
                FROM
                    instance_waste_normal
            ) before
        ON
            after.afterYear = before.beforeYear - 5
            AND after.region = before.region
            AND after.type = before.type
    ) unioned