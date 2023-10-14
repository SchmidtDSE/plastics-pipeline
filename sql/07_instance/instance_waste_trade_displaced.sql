CREATE VIEW instance_waste_trade_displaced AS
SELECT
    unioned.years AS years,
    unioned.popChange AS popChange,
    unioned.gdpChange AS gdpChange,
    unioned.afterGdp AS afterGdp,
    unioned.afterPopulation AS afterPopulation,
    unioned.beforeNetMT / unioned.beforeTotalConsumption AS beforePercent,
    unioned.afterNetMT / unioned.afterTotalConsumption AS afterPercent,
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
            WHEN unioned.region = 'china' AND year >= 2017 THEN 1
            ELSE 0
        END
    ) AS flagSword,
    unioned.beforeYear AS beforeYear,
    unioned.afterYear AS afterYear,
    unioned.afterTotalConsumption AS afterTotalConsumption,
    unioned.beforeTotalConsumption AS beforeTotalConsumption
FROM
    (
        SELECT
            abs(after.afterYear - before.beforeYear) AS years,
            (afterPopulation - beforePopulation) / beforePopulation AS popChange,
            (afterGdp / afterPopulation - beforeGdp / beforePopulation) / (beforeGdp / beforePopulation) AS gdpChange,
            after.region AS region,
            after.afterGdp AS afterGdp,
            after.afterPopulation AS afterPopulation,
            before.beforeNetMT AS beforeNetMT,
            after.afterNetMT AS afterNetMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear,
            after.afterTotalConsumption AS afterTotalConsumption,
            before.beforeTotalConsumption AS beforeTotalConsumption
        FROM
            (
                SELECT
                    instance_waste_trade_normal.year AS afterYear,
                    instance_waste_trade_normal.region AS region,
                    instance_waste_trade_normal.population AS afterPopulation,
                    instance_waste_trade_normal.gdp AS afterGdp,
                    instance_waste_trade_normal.netMT AS afterNetMT,
                    instance_waste_trade_normal.totalConsumption AS afterTotalConsumption
                FROM
                    instance_waste_trade_normal
            ) after
        INNER JOIN
            (
                SELECT
                    instance_waste_trade_normal.year AS beforeYear,
                    instance_waste_trade_normal.region AS region,
                    instance_waste_trade_normal.population AS beforePopulation,
                    instance_waste_trade_normal.gdp AS beforeGdp,
                    instance_waste_trade_normal.netMT AS beforeNetMT,
                    instance_waste_trade_normal.totalConsumption AS beforeTotalConsumption
                FROM
                    instance_waste_trade_normal
            ) before
        ON
            after.afterYear = before.beforeYear + 1
            AND after.region = before.region
        UNION ALL
        SELECT
            abs(after.afterYear - before.beforeYear) AS years,
            (afterPopulation - beforePopulation) / beforePopulation AS popChange,
            (afterGdp / afterPopulation - beforeGdp / beforePopulation) / (beforeGdp / beforePopulation) AS gdpChange,
            after.region AS region,
            after.afterGdp AS afterGdp,
            after.afterPopulation AS afterPopulation,
            before.beforeNetMT AS beforeNetMT,
            after.afterNetMT AS afterNetMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear,
            after.afterTotalConsumption AS afterTotalConsumption,
            before.beforeTotalConsumption AS beforeTotalConsumption
        FROM
            (
                SELECT
                    instance_waste_trade_normal.year AS afterYear,
                    instance_waste_trade_normal.region AS region,
                    instance_waste_trade_normal.population AS afterPopulation,
                    instance_waste_trade_normal.gdp AS afterGdp,
                    instance_waste_trade_normal.netMT AS afterNetMT,
                    instance_waste_trade_normal.totalConsumption AS afterTotalConsumption
                FROM
                    instance_waste_trade_normal
            ) after
        INNER JOIN
            (
                SELECT
                    instance_waste_trade_normal.year AS beforeYear,
                    instance_waste_trade_normal.region AS region,
                    instance_waste_trade_normal.population AS beforePopulation,
                    instance_waste_trade_normal.gdp AS beforeGdp,
                    instance_waste_trade_normal.netMT AS beforeNetMT,
                    instance_waste_trade_normal.totalConsumption AS beforeTotalConsumption
                FROM
                    instance_waste_trade_normal
            ) before
        ON
            after.afterYear = before.beforeYear + 2
            AND after.region = before.region
        UNION ALL
        SELECT
            abs(after.afterYear - before.beforeYear) AS years,
            (afterPopulation - beforePopulation) / beforePopulation AS popChange,
            (afterGdp / afterPopulation - beforeGdp / beforePopulation) / (beforeGdp / beforePopulation) AS gdpChange,
            after.region AS region,
            after.afterGdp AS afterGdp,
            after.afterPopulation AS afterPopulation,
            before.beforeNetMT AS beforeNetMT,
            after.afterNetMT AS afterNetMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear,
            after.afterTotalConsumption AS afterTotalConsumption,
            before.beforeTotalConsumption AS beforeTotalConsumption
        FROM
            (
                SELECT
                    instance_waste_trade_normal.year AS afterYear,
                    instance_waste_trade_normal.region AS region,
                    instance_waste_trade_normal.population AS afterPopulation,
                    instance_waste_trade_normal.gdp AS afterGdp,
                    instance_waste_trade_normal.netMT AS afterNetMT,
                    instance_waste_trade_normal.totalConsumption AS afterTotalConsumption
                FROM
                    instance_waste_trade_normal
            ) after
        INNER JOIN
            (
                SELECT
                    instance_waste_trade_normal.year AS beforeYear,
                    instance_waste_trade_normal.region AS region,
                    instance_waste_trade_normal.population AS beforePopulation,
                    instance_waste_trade_normal.gdp AS beforeGdp,
                    instance_waste_trade_normal.netMT AS beforeNetMT,
                    instance_waste_trade_normal.totalConsumption AS beforeTotalConsumption
                FROM
                    instance_waste_trade_normal
            ) before
        ON
            after.afterYear = before.beforeYear + 3
            AND after.region = before.region
        UNION ALL
        SELECT
            abs(after.afterYear - before.beforeYear) AS years,
            (afterPopulation - beforePopulation) / beforePopulation AS popChange,
            (afterGdp / afterPopulation - beforeGdp / beforePopulation) / (beforeGdp / beforePopulation) AS gdpChange,
            after.region AS region,
            after.afterGdp AS afterGdp,
            after.afterPopulation AS afterPopulation,
            before.beforeNetMT AS beforeNetMT,
            after.afterNetMT AS afterNetMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear,
            after.afterTotalConsumption AS afterTotalConsumption,
            before.beforeTotalConsumption AS beforeTotalConsumption
        FROM
            (
                SELECT
                    instance_waste_trade_normal.year AS afterYear,
                    instance_waste_trade_normal.region AS region,
                    instance_waste_trade_normal.population AS afterPopulation,
                    instance_waste_trade_normal.gdp AS afterGdp,
                    instance_waste_trade_normal.netMT AS afterNetMT,
                    instance_waste_trade_normal.totalConsumption AS afterTotalConsumption
                FROM
                    instance_waste_trade_normal
            ) after
        INNER JOIN
            (
                SELECT
                    instance_waste_trade_normal.year AS beforeYear,
                    instance_waste_trade_normal.region AS region,
                    instance_waste_trade_normal.population AS beforePopulation,
                    instance_waste_trade_normal.gdp AS beforeGdp,
                    instance_waste_trade_normal.netMT AS beforeNetMT,
                    instance_waste_trade_normal.totalConsumption AS beforeTotalConsumption
                FROM
                    instance_waste_trade_normal
            ) before
        ON
            after.afterYear = before.beforeYear + 4
            AND after.region = before.region
        UNION ALL
        SELECT
            abs(after.afterYear - before.beforeYear) AS years,
            (afterPopulation - beforePopulation) / beforePopulation AS popChange,
            (afterGdp / afterPopulation - beforeGdp / beforePopulation) / (beforeGdp / beforePopulation) AS gdpChange,
            after.region AS region,
            after.afterGdp AS afterGdp,
            after.afterPopulation AS afterPopulation,
            before.beforeNetMT AS beforeNetMT,
            after.afterNetMT AS afterNetMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear,
            after.afterTotalConsumption AS afterTotalConsumption,
            before.beforeTotalConsumption AS beforeTotalConsumption
        FROM
            (
                SELECT
                    instance_waste_trade_normal.year AS afterYear,
                    instance_waste_trade_normal.region AS region,
                    instance_waste_trade_normal.population AS afterPopulation,
                    instance_waste_trade_normal.gdp AS afterGdp,
                    instance_waste_trade_normal.netMT AS afterNetMT,
                    instance_waste_trade_normal.totalConsumption AS afterTotalConsumption
                FROM
                    instance_waste_trade_normal
            ) after
        INNER JOIN
            (
                SELECT
                    instance_waste_trade_normal.year AS beforeYear,
                    instance_waste_trade_normal.region AS region,
                    instance_waste_trade_normal.population AS beforePopulation,
                    instance_waste_trade_normal.gdp AS beforeGdp,
                    instance_waste_trade_normal.netMT AS beforeNetMT,
                    instance_waste_trade_normal.totalConsumption AS beforeTotalConsumption
                FROM
                    instance_waste_trade_normal
            ) before
        ON
            after.afterYear = before.beforeYear + 5
            AND after.region = before.region
        UNION ALL
        SELECT
            abs(after.afterYear - before.beforeYear) AS years,
            (afterPopulation - beforePopulation) / beforePopulation AS popChange,
            (afterGdp / afterPopulation - beforeGdp / beforePopulation) / (beforeGdp / beforePopulation) AS gdpChange,
            after.region AS region,
            after.afterGdp AS afterGdp,
            after.afterPopulation AS afterPopulation,
            before.beforeNetMT AS beforeNetMT,
            after.afterNetMT AS afterNetMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear,
            after.afterTotalConsumption AS afterTotalConsumption,
            before.beforeTotalConsumption AS beforeTotalConsumption
        FROM
            (
                SELECT
                    instance_waste_trade_normal.year AS afterYear,
                    instance_waste_trade_normal.region AS region,
                    instance_waste_trade_normal.population AS afterPopulation,
                    instance_waste_trade_normal.gdp AS afterGdp,
                    instance_waste_trade_normal.netMT AS afterNetMT,
                    instance_waste_trade_normal.totalConsumption AS afterTotalConsumption
                FROM
                    instance_waste_trade_normal
            ) after
        INNER JOIN
            (
                SELECT
                    instance_waste_trade_normal.year AS beforeYear,
                    instance_waste_trade_normal.region AS region,
                    instance_waste_trade_normal.population AS beforePopulation,
                    instance_waste_trade_normal.gdp AS beforeGdp,
                    instance_waste_trade_normal.netMT AS beforeNetMT,
                    instance_waste_trade_normal.totalConsumption AS beforeTotalConsumption
                FROM
                    instance_waste_trade_normal
            ) before
        ON
            after.afterYear = before.beforeYear - 1
            AND after.region = before.region
        UNION ALL
        SELECT
            abs(after.afterYear - before.beforeYear) AS years,
            (afterPopulation - beforePopulation) / beforePopulation AS popChange,
            (afterGdp / afterPopulation - beforeGdp / beforePopulation) / (beforeGdp / beforePopulation) AS gdpChange,
            after.region AS region,
            after.afterGdp AS afterGdp,
            after.afterPopulation AS afterPopulation,
            before.beforeNetMT AS beforeNetMT,
            after.afterNetMT AS afterNetMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear,
            after.afterTotalConsumption AS afterTotalConsumption,
            before.beforeTotalConsumption AS beforeTotalConsumption
        FROM
            (
                SELECT
                    instance_waste_trade_normal.year AS afterYear,
                    instance_waste_trade_normal.region AS region,
                    instance_waste_trade_normal.population AS afterPopulation,
                    instance_waste_trade_normal.gdp AS afterGdp,
                    instance_waste_trade_normal.netMT AS afterNetMT,
                    instance_waste_trade_normal.totalConsumption AS afterTotalConsumption
                FROM
                    instance_waste_trade_normal
            ) after
        INNER JOIN
            (
                SELECT
                    instance_waste_trade_normal.year AS beforeYear,
                    instance_waste_trade_normal.region AS region,
                    instance_waste_trade_normal.population AS beforePopulation,
                    instance_waste_trade_normal.gdp AS beforeGdp,
                    instance_waste_trade_normal.netMT AS beforeNetMT,
                    instance_waste_trade_normal.totalConsumption AS beforeTotalConsumption
                FROM
                    instance_waste_trade_normal
            ) before
        ON
            after.afterYear = before.beforeYear - 2
            AND after.region = before.region
        UNION ALL
        SELECT
            abs(after.afterYear - before.beforeYear) AS years,
            (afterPopulation - beforePopulation) / beforePopulation AS popChange,
            (afterGdp / afterPopulation - beforeGdp / beforePopulation) / (beforeGdp / beforePopulation) AS gdpChange,
            after.region AS region,
            after.afterGdp AS afterGdp,
            after.afterPopulation AS afterPopulation,
            before.beforeNetMT AS beforeNetMT,
            after.afterNetMT AS afterNetMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear,
            after.afterTotalConsumption AS afterTotalConsumption,
            before.beforeTotalConsumption AS beforeTotalConsumption
        FROM
            (
                SELECT
                    instance_waste_trade_normal.year AS afterYear,
                    instance_waste_trade_normal.region AS region,
                    instance_waste_trade_normal.population AS afterPopulation,
                    instance_waste_trade_normal.gdp AS afterGdp,
                    instance_waste_trade_normal.netMT AS afterNetMT,
                    instance_waste_trade_normal.totalConsumption AS afterTotalConsumption
                FROM
                    instance_waste_trade_normal
            ) after
        INNER JOIN
            (
                SELECT
                    instance_waste_trade_normal.year AS beforeYear,
                    instance_waste_trade_normal.region AS region,
                    instance_waste_trade_normal.population AS beforePopulation,
                    instance_waste_trade_normal.gdp AS beforeGdp,
                    instance_waste_trade_normal.netMT AS beforeNetMT,
                    instance_waste_trade_normal.totalConsumption AS beforeTotalConsumption
                FROM
                    instance_waste_trade_normal
            ) before
        ON
            after.afterYear = before.beforeYear - 3
            AND after.region = before.region
        UNION ALL
        SELECT
            abs(after.afterYear - before.beforeYear) AS years,
            (afterPopulation - beforePopulation) / beforePopulation AS popChange,
            (afterGdp / afterPopulation - beforeGdp / beforePopulation) / (beforeGdp / beforePopulation) AS gdpChange,
            after.region AS region,
            after.afterGdp AS afterGdp,
            after.afterPopulation AS afterPopulation,
            before.beforeNetMT AS beforeNetMT,
            after.afterNetMT AS afterNetMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear,
            after.afterTotalConsumption AS afterTotalConsumption,
            before.beforeTotalConsumption AS beforeTotalConsumption
        FROM
            (
                SELECT
                    instance_waste_trade_normal.year AS afterYear,
                    instance_waste_trade_normal.region AS region,
                    instance_waste_trade_normal.population AS afterPopulation,
                    instance_waste_trade_normal.gdp AS afterGdp,
                    instance_waste_trade_normal.netMT AS afterNetMT,
                    instance_waste_trade_normal.totalConsumption AS afterTotalConsumption
                FROM
                    instance_waste_trade_normal
            ) after
        INNER JOIN
            (
                SELECT
                    instance_waste_trade_normal.year AS beforeYear,
                    instance_waste_trade_normal.region AS region,
                    instance_waste_trade_normal.population AS beforePopulation,
                    instance_waste_trade_normal.gdp AS beforeGdp,
                    instance_waste_trade_normal.netMT AS beforeNetMT,
                    instance_waste_trade_normal.totalConsumption AS beforeTotalConsumption
                FROM
                    instance_waste_trade_normal
            ) before
        ON
            after.afterYear = before.beforeYear - 4
            AND after.region = before.region
        UNION ALL
        SELECT
            abs(after.afterYear - before.beforeYear) AS years,
            (afterPopulation - beforePopulation) / beforePopulation AS popChange,
            (afterGdp / afterPopulation - beforeGdp / beforePopulation) / (beforeGdp / beforePopulation) AS gdpChange,
            after.region AS region,
            after.afterGdp AS afterGdp,
            after.afterPopulation AS afterPopulation,
            before.beforeNetMT AS beforeNetMT,
            after.afterNetMT AS afterNetMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear,
            after.afterTotalConsumption AS afterTotalConsumption,
            before.beforeTotalConsumption AS beforeTotalConsumption
        FROM
            (
                SELECT
                    instance_waste_trade_normal.year AS afterYear,
                    instance_waste_trade_normal.region AS region,
                    instance_waste_trade_normal.population AS afterPopulation,
                    instance_waste_trade_normal.gdp AS afterGdp,
                    instance_waste_trade_normal.netMT AS afterNetMT,
                    instance_waste_trade_normal.totalConsumption AS afterTotalConsumption
                FROM
                    instance_waste_trade_normal
            ) after
        INNER JOIN
            (
                SELECT
                    instance_waste_trade_normal.year AS beforeYear,
                    instance_waste_trade_normal.region AS region,
                    instance_waste_trade_normal.population AS beforePopulation,
                    instance_waste_trade_normal.gdp AS beforeGdp,
                    instance_waste_trade_normal.netMT AS beforeNetMT,
                    instance_waste_trade_normal.totalConsumption AS beforeTotalConsumption
                FROM
                    instance_waste_trade_normal
            ) before
        ON
            after.afterYear = before.beforeYear - 5
            AND after.region = before.region
    ) unioned