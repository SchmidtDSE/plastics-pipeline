CREATE VIEW instance_trade_displaced AS
SELECT
    unioned.years AS years,
    unioned.popChange AS popChange,
    unioned.gdpChange AS gdpChange,
    unioned.afterGdp AS afterGdp,
    unioned.afterPopulation AS afterPopulation,
    {% for region in regions %}
    (
        CASE
            WHEN unioned.region = '{{ region["key"] }}' THEN 1
            ELSE 0
        END
    ) AS flag{{ region["sqlSuffix"] }},
    {% endfor %}
    (
        CASE
            WHEN unioned.type = 'articles' THEN 1
            ELSE 0
        END
    ) AS flagArticles,
    (
        CASE
            WHEN unioned.type = 'fibers' THEN 1
            ELSE 0
        END
    ) AS flagFibers,
    (
        CASE
            WHEN unioned.type = 'goods' THEN 1
            ELSE 0
        END
    ) AS flagGoods,
    (
        CASE
            WHEN unioned.type = 'resin' THEN 1
            ELSE 0
        END
    ) AS flagResin,
    unioned.beforeNetMT / unioned.beforeTotalConsumption AS beforePercent,
    unioned.afterNetMT / unioned.afterTotalConsumption AS afterPercent,
    unioned.beforeYear AS beforeYear,
    unioned.afterYear AS afterYear,
    unioned.beforeTotalConsumption AS beforeTotalConsumption,
    unioned.afterTotalConsumption AS afterTotalConsumption
FROM
    (
        SELECT
            abs(after.afterYear - before.beforeYear) AS years,
            (afterPopulation - beforePopulation) / beforePopulation AS popChange,
            (afterGdp / afterPopulation - beforeGdp / beforePopulation) / (beforeGdp / beforePopulation) AS gdpChange,
            after.region AS region,
            after.type AS type,
            after.afterGdp AS afterGdp,
            after.afterPopulation AS afterPopulation,
            before.beforeNetMT AS beforeNetMT,
            after.afterNetMT AS afterNetMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear,
            before.beforeTotalConsumption AS beforeTotalConsumption,
            after.afterTotalConsumption AS afterTotalConsumption
        FROM
            (
                SELECT
                    instance_trade_normal.year AS afterYear,
                    instance_trade_normal.region AS region,
                    instance_trade_normal.type AS type,
                    instance_trade_normal.population AS afterPopulation,
                    instance_trade_normal.gdp AS afterGdp,
                    instance_trade_normal.netMT AS afterNetMT,
                    instance_trade_normal.totalConsumption AS afterTotalConsumption
                FROM
                    instance_trade_normal
            ) after
        INNER JOIN
            (
                SELECT
                    instance_trade_normal.year AS beforeYear,
                    instance_trade_normal.region AS region,
                    instance_trade_normal.type AS type,
                    instance_trade_normal.population AS beforePopulation,
                    instance_trade_normal.gdp AS beforeGdp,
                    instance_trade_normal.netMT AS beforeNetMT,
                    instance_trade_normal.totalConsumption AS beforeTotalConsumption
                FROM
                    instance_trade_normal
            ) before
        ON
            after.afterYear = before.beforeYear + 1
            AND after.region = before.region
            AND after.type = before.type
        UNION ALL
        SELECT
            abs(after.afterYear - before.beforeYear) AS years,
            (afterPopulation - beforePopulation) / beforePopulation AS popChange,
            (afterGdp / afterPopulation - beforeGdp / beforePopulation) / (beforeGdp / beforePopulation) AS gdpChange,
            after.region AS region,
            after.type AS type,
            after.afterGdp AS afterGdp,
            after.afterPopulation AS afterPopulation,
            before.beforeNetMT AS beforeNetMT,
            after.afterNetMT AS afterNetMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear,
            before.beforeTotalConsumption AS beforeTotalConsumption,
            after.afterTotalConsumption AS afterTotalConsumption
        FROM
            (
                SELECT
                    instance_trade_normal.year AS afterYear,
                    instance_trade_normal.region AS region,
                    instance_trade_normal.type AS type,
                    instance_trade_normal.population AS afterPopulation,
                    instance_trade_normal.gdp AS afterGdp,
                    instance_trade_normal.netMT AS afterNetMT,
                    instance_trade_normal.totalConsumption AS afterTotalConsumption
                FROM
                    instance_trade_normal
            ) after
        INNER JOIN
            (
                SELECT
                    instance_trade_normal.year AS beforeYear,
                    instance_trade_normal.region AS region,
                    instance_trade_normal.type AS type,
                    instance_trade_normal.population AS beforePopulation,
                    instance_trade_normal.gdp AS beforeGdp,
                    instance_trade_normal.netMT AS beforeNetMT,
                    instance_trade_normal.totalConsumption AS beforeTotalConsumption
                FROM
                    instance_trade_normal
            ) before
        ON
            after.afterYear = before.beforeYear + 2
            AND after.region = before.region
            AND after.type = before.type
        UNION ALL
        SELECT
            abs(after.afterYear - before.beforeYear) AS years,
            (afterPopulation - beforePopulation) / beforePopulation AS popChange,
            (afterGdp / afterPopulation - beforeGdp / beforePopulation) / (beforeGdp / beforePopulation) AS gdpChange,
            after.region AS region,
            after.type AS type,
            after.afterGdp AS afterGdp,
            after.afterPopulation AS afterPopulation,
            before.beforeNetMT AS beforeNetMT,
            after.afterNetMT AS afterNetMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear,
            before.beforeTotalConsumption AS beforeTotalConsumption,
            after.afterTotalConsumption AS afterTotalConsumption
        FROM
            (
                SELECT
                    instance_trade_normal.year AS afterYear,
                    instance_trade_normal.region AS region,
                    instance_trade_normal.type AS type,
                    instance_trade_normal.population AS afterPopulation,
                    instance_trade_normal.gdp AS afterGdp,
                    instance_trade_normal.netMT AS afterNetMT,
                    instance_trade_normal.totalConsumption AS afterTotalConsumption
                FROM
                    instance_trade_normal
            ) after
        INNER JOIN
            (
                SELECT
                    instance_trade_normal.year AS beforeYear,
                    instance_trade_normal.region AS region,
                    instance_trade_normal.type AS type,
                    instance_trade_normal.population AS beforePopulation,
                    instance_trade_normal.gdp AS beforeGdp,
                    instance_trade_normal.netMT AS beforeNetMT,
                    instance_trade_normal.totalConsumption AS beforeTotalConsumption
                FROM
                    instance_trade_normal
            ) before
        ON
            after.afterYear = before.beforeYear + 3
            AND after.region = before.region
            AND after.type = before.type
        UNION ALL
        SELECT
            abs(after.afterYear - before.beforeYear) AS years,
            (afterPopulation - beforePopulation) / beforePopulation AS popChange,
            (afterGdp / afterPopulation - beforeGdp / beforePopulation) / (beforeGdp / beforePopulation) AS gdpChange,
            after.region AS region,
            after.type AS type,
            after.afterGdp AS afterGdp,
            after.afterPopulation AS afterPopulation,
            before.beforeNetMT AS beforeNetMT,
            after.afterNetMT AS afterNetMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear,
            before.beforeTotalConsumption AS beforeTotalConsumption,
            after.afterTotalConsumption AS afterTotalConsumption
        FROM
            (
                SELECT
                    instance_trade_normal.year AS afterYear,
                    instance_trade_normal.region AS region,
                    instance_trade_normal.type AS type,
                    instance_trade_normal.population AS afterPopulation,
                    instance_trade_normal.gdp AS afterGdp,
                    instance_trade_normal.netMT AS afterNetMT,
                    instance_trade_normal.totalConsumption AS afterTotalConsumption
                FROM
                    instance_trade_normal
            ) after
        INNER JOIN
            (
                SELECT
                    instance_trade_normal.year AS beforeYear,
                    instance_trade_normal.region AS region,
                    instance_trade_normal.type AS type,
                    instance_trade_normal.population AS beforePopulation,
                    instance_trade_normal.gdp AS beforeGdp,
                    instance_trade_normal.netMT AS beforeNetMT,
                    instance_trade_normal.totalConsumption AS beforeTotalConsumption
                FROM
                    instance_trade_normal
            ) before
        ON
            after.afterYear = before.beforeYear + 4
            AND after.region = before.region
            AND after.type = before.type
        UNION ALL
        SELECT
            abs(after.afterYear - before.beforeYear) AS years,
            (afterPopulation - beforePopulation) / beforePopulation AS popChange,
            (afterGdp / afterPopulation - beforeGdp / beforePopulation) / (beforeGdp / beforePopulation) AS gdpChange,
            after.region AS region,
            after.type AS type,
            after.afterGdp AS afterGdp,
            after.afterPopulation AS afterPopulation,
            before.beforeNetMT AS beforeNetMT,
            after.afterNetMT AS afterNetMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear,
            before.beforeTotalConsumption AS beforeTotalConsumption,
            after.afterTotalConsumption AS afterTotalConsumption
        FROM
            (
                SELECT
                    instance_trade_normal.year AS afterYear,
                    instance_trade_normal.region AS region,
                    instance_trade_normal.type AS type,
                    instance_trade_normal.population AS afterPopulation,
                    instance_trade_normal.gdp AS afterGdp,
                    instance_trade_normal.netMT AS afterNetMT,
                    instance_trade_normal.totalConsumption AS afterTotalConsumption
                FROM
                    instance_trade_normal
            ) after
        INNER JOIN
            (
                SELECT
                    instance_trade_normal.year AS beforeYear,
                    instance_trade_normal.region AS region,
                    instance_trade_normal.type AS type,
                    instance_trade_normal.population AS beforePopulation,
                    instance_trade_normal.gdp AS beforeGdp,
                    instance_trade_normal.netMT AS beforeNetMT,
                    instance_trade_normal.totalConsumption AS beforeTotalConsumption
                FROM
                    instance_trade_normal
            ) before
        ON
            after.afterYear = before.beforeYear + 5
            AND after.region = before.region
            AND after.type = before.type
        UNION ALL
        SELECT
            abs(after.afterYear - before.beforeYear) AS years,
            (afterPopulation - beforePopulation) / beforePopulation AS popChange,
            (afterGdp / afterPopulation - beforeGdp / beforePopulation) / (beforeGdp / beforePopulation) AS gdpChange,
            after.region AS region,
            after.type AS type,
            after.afterGdp AS afterGdp,
            after.afterPopulation AS afterPopulation,
            before.beforeNetMT AS beforeNetMT,
            after.afterNetMT AS afterNetMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear,
            before.beforeTotalConsumption AS beforeTotalConsumption,
            after.afterTotalConsumption AS afterTotalConsumption
        FROM
            (
                SELECT
                    instance_trade_normal.year AS afterYear,
                    instance_trade_normal.region AS region,
                    instance_trade_normal.type AS type,
                    instance_trade_normal.population AS afterPopulation,
                    instance_trade_normal.gdp AS afterGdp,
                    instance_trade_normal.netMT AS afterNetMT,
                    instance_trade_normal.totalConsumption AS afterTotalConsumption
                FROM
                    instance_trade_normal
            ) after
        INNER JOIN
            (
                SELECT
                    instance_trade_normal.year AS beforeYear,
                    instance_trade_normal.region AS region,
                    instance_trade_normal.type AS type,
                    instance_trade_normal.population AS beforePopulation,
                    instance_trade_normal.gdp AS beforeGdp,
                    instance_trade_normal.netMT AS beforeNetMT,
                    instance_trade_normal.totalConsumption AS beforeTotalConsumption
                FROM
                    instance_trade_normal
            ) before
        ON
            after.afterYear = before.beforeYear - 1
            AND after.region = before.region
            AND after.type = before.type
        UNION ALL
        SELECT
            abs(after.afterYear - before.beforeYear) AS years,
            (afterPopulation - beforePopulation) / beforePopulation AS popChange,
            (afterGdp / afterPopulation - beforeGdp / beforePopulation) / (beforeGdp / beforePopulation) AS gdpChange,
            after.region AS region,
            after.type AS type,
            after.afterGdp AS afterGdp,
            after.afterPopulation AS afterPopulation,
            before.beforeNetMT AS beforeNetMT,
            after.afterNetMT AS afterNetMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear,
            before.beforeTotalConsumption AS beforeTotalConsumption,
            after.afterTotalConsumption AS afterTotalConsumption
        FROM
            (
                SELECT
                    instance_trade_normal.year AS afterYear,
                    instance_trade_normal.region AS region,
                    instance_trade_normal.type AS type,
                    instance_trade_normal.population AS afterPopulation,
                    instance_trade_normal.gdp AS afterGdp,
                    instance_trade_normal.netMT AS afterNetMT,
                    instance_trade_normal.totalConsumption AS afterTotalConsumption
                FROM
                    instance_trade_normal
            ) after
        INNER JOIN
            (
                SELECT
                    instance_trade_normal.year AS beforeYear,
                    instance_trade_normal.region AS region,
                    instance_trade_normal.type AS type,
                    instance_trade_normal.population AS beforePopulation,
                    instance_trade_normal.gdp AS beforeGdp,
                    instance_trade_normal.netMT AS beforeNetMT,
                    instance_trade_normal.totalConsumption AS beforeTotalConsumption
                FROM
                    instance_trade_normal
            ) before
        ON
            after.afterYear = before.beforeYear - 2
            AND after.region = before.region
            AND after.type = before.type
        UNION ALL
        SELECT
            abs(after.afterYear - before.beforeYear) AS years,
            (afterPopulation - beforePopulation) / beforePopulation AS popChange,
            (afterGdp / afterPopulation - beforeGdp / beforePopulation) / (beforeGdp / beforePopulation) AS gdpChange,
            after.region AS region,
            after.type AS type,
            after.afterGdp AS afterGdp,
            after.afterPopulation AS afterPopulation,
            before.beforeNetMT AS beforeNetMT,
            after.afterNetMT AS afterNetMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear,
            before.beforeTotalConsumption AS beforeTotalConsumption,
            after.afterTotalConsumption AS afterTotalConsumption
        FROM
            (
                SELECT
                    instance_trade_normal.year AS afterYear,
                    instance_trade_normal.region AS region,
                    instance_trade_normal.type AS type,
                    instance_trade_normal.population AS afterPopulation,
                    instance_trade_normal.gdp AS afterGdp,
                    instance_trade_normal.netMT AS afterNetMT,
                    instance_trade_normal.totalConsumption AS afterTotalConsumption
                FROM
                    instance_trade_normal
            ) after
        INNER JOIN
            (
                SELECT
                    instance_trade_normal.year AS beforeYear,
                    instance_trade_normal.region AS region,
                    instance_trade_normal.type AS type,
                    instance_trade_normal.population AS beforePopulation,
                    instance_trade_normal.gdp AS beforeGdp,
                    instance_trade_normal.netMT AS beforeNetMT,
                    instance_trade_normal.totalConsumption AS beforeTotalConsumption
                FROM
                    instance_trade_normal
            ) before
        ON
            after.afterYear = before.beforeYear - 3
            AND after.region = before.region
            AND after.type = before.type
        UNION ALL
        SELECT
            abs(after.afterYear - before.beforeYear) AS years,
            (afterPopulation - beforePopulation) / beforePopulation AS popChange,
            (afterGdp / afterPopulation - beforeGdp / beforePopulation) / (beforeGdp / beforePopulation) AS gdpChange,
            after.region AS region,
            after.type AS type,
            after.afterGdp AS afterGdp,
            after.afterPopulation AS afterPopulation,
            before.beforeNetMT AS beforeNetMT,
            after.afterNetMT AS afterNetMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear,
            before.beforeTotalConsumption AS beforeTotalConsumption,
            after.afterTotalConsumption AS afterTotalConsumption
        FROM
            (
                SELECT
                    instance_trade_normal.year AS afterYear,
                    instance_trade_normal.region AS region,
                    instance_trade_normal.type AS type,
                    instance_trade_normal.population AS afterPopulation,
                    instance_trade_normal.gdp AS afterGdp,
                    instance_trade_normal.netMT AS afterNetMT,
                    instance_trade_normal.totalConsumption AS afterTotalConsumption
                FROM
                    instance_trade_normal
            ) after
        INNER JOIN
            (
                SELECT
                    instance_trade_normal.year AS beforeYear,
                    instance_trade_normal.region AS region,
                    instance_trade_normal.type AS type,
                    instance_trade_normal.population AS beforePopulation,
                    instance_trade_normal.gdp AS beforeGdp,
                    instance_trade_normal.netMT AS beforeNetMT,
                    instance_trade_normal.totalConsumption AS beforeTotalConsumption
                FROM
                    instance_trade_normal
            ) before
        ON
            after.afterYear = before.beforeYear - 4
            AND after.region = before.region
            AND after.type = before.type
        UNION ALL
        SELECT
            abs(after.afterYear - before.beforeYear) AS years,
            (afterPopulation - beforePopulation) / beforePopulation AS popChange,
            (afterGdp / afterPopulation - beforeGdp / beforePopulation) / (beforeGdp / beforePopulation) AS gdpChange,
            after.region AS region,
            after.type AS type,
            after.afterGdp AS afterGdp,
            after.afterPopulation AS afterPopulation,
            before.beforeNetMT AS beforeNetMT,
            after.afterNetMT AS afterNetMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear,
            before.beforeTotalConsumption AS beforeTotalConsumption,
            after.afterTotalConsumption AS afterTotalConsumption
        FROM
            (
                SELECT
                    instance_trade_normal.year AS afterYear,
                    instance_trade_normal.region AS region,
                    instance_trade_normal.type AS type,
                    instance_trade_normal.population AS afterPopulation,
                    instance_trade_normal.gdp AS afterGdp,
                    instance_trade_normal.netMT AS afterNetMT,
                    instance_trade_normal.totalConsumption AS afterTotalConsumption
                FROM
                    instance_trade_normal
            ) after
        INNER JOIN
            (
                SELECT
                    instance_trade_normal.year AS beforeYear,
                    instance_trade_normal.region AS region,
                    instance_trade_normal.type AS type,
                    instance_trade_normal.population AS beforePopulation,
                    instance_trade_normal.gdp AS beforeGdp,
                    instance_trade_normal.netMT AS beforeNetMT,
                    instance_trade_normal.totalConsumption AS beforeTotalConsumption
                FROM
                    instance_trade_normal
            ) before
        ON
            after.afterYear = before.beforeYear - 5
            AND after.region = before.region
            AND after.type = before.type
    ) unioned