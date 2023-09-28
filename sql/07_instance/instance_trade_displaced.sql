CREATE VIEW instance_trade_displaced AS
SELECT
    unioned.years AS years,
    unioned.popChange AS popChange,
    unioned.gdpPerCapChange AS gdpPerCapChange,
    unioned.afterGdp AS afterGdp,
    unioned.afterPopulation AS afterPopulation,
    unioned.beforeNetMT AS beforeNetMT,
    unioned.netMTChange AS netMTChange,
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
    unioned.afterNetMT AS afterNetMT,
    unioned.beforeYear AS beforeYear,
    unioned.afterYear AS afterYear
FROM
    (
        SELECT
            abs(after.afterYear - before.beforeYear) AS years,
            (afterPopulation - beforePopulation) / beforePopulation AS popChange,
            (afterGdp / afterPopulation - beforeGdp / beforePopulation) / (beforeGdp / beforePopulation) AS gdpPerCapChange,
            (afterNetMT - beforeNetMT) / beforeNetMT AS netMTChange,
            after.region AS region,
            after.type AS type,
            after.afterGdp AS afterGdp,
            after.afterPopulation AS afterPopulation,
            before.beforeNetMT AS beforeNetMT,
            after.afterNetMT AS afterNetMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear
        FROM
            (
                SELECT
                    instance_trade_normal.year AS afterYear,
                    instance_trade_normal.region AS region,
                    instance_trade_normal.type AS type,
                    instance_trade_normal.population AS afterPopulation,
                    instance_trade_normal.gdp AS afterGdp,
                    instance_trade_normal.netMT AS afterNetMT
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
                    instance_trade_normal.netMT AS beforeNetMT
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
            (afterGdp / afterPopulation - beforeGdp / beforePopulation) / (beforeGdp / beforePopulation) AS gdpPerCapChange,
            (afterNetMT - beforeNetMT) / beforeNetMT AS netMTChange,
            after.region AS region,
            after.type AS type,
            after.afterGdp AS afterGdp,
            after.afterPopulation AS afterPopulation,
            before.beforeNetMT AS beforeNetMT,
            after.afterNetMT AS afterNetMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear
        FROM
            (
                SELECT
                    instance_trade_normal.year AS afterYear,
                    instance_trade_normal.region AS region,
                    instance_trade_normal.type AS type,
                    instance_trade_normal.population AS afterPopulation,
                    instance_trade_normal.gdp AS afterGdp,
                    instance_trade_normal.netMT AS afterNetMT
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
                    instance_trade_normal.netMT AS beforeNetMT
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
            (afterGdp / afterPopulation - beforeGdp / beforePopulation) / (beforeGdp / beforePopulation) AS gdpPerCapChange,
            (afterNetMT - beforeNetMT) / beforeNetMT AS netMTChange,
            after.region AS region,
            after.type AS type,
            after.afterGdp AS afterGdp,
            after.afterPopulation AS afterPopulation,
            before.beforeNetMT AS beforeNetMT,
            after.afterNetMT AS afterNetMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear
        FROM
            (
                SELECT
                    instance_trade_normal.year AS afterYear,
                    instance_trade_normal.region AS region,
                    instance_trade_normal.type AS type,
                    instance_trade_normal.population AS afterPopulation,
                    instance_trade_normal.gdp AS afterGdp,
                    instance_trade_normal.netMT AS afterNetMT
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
                    instance_trade_normal.netMT AS beforeNetMT
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
            (afterGdp / afterPopulation - beforeGdp / beforePopulation) / (beforeGdp / beforePopulation) AS gdpPerCapChange,
            (afterNetMT - beforeNetMT) / beforeNetMT AS netMTChange,
            after.region AS region,
            after.type AS type,
            after.afterGdp AS afterGdp,
            after.afterPopulation AS afterPopulation,
            before.beforeNetMT AS beforeNetMT,
            after.afterNetMT AS afterNetMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear
        FROM
            (
                SELECT
                    instance_trade_normal.year AS afterYear,
                    instance_trade_normal.region AS region,
                    instance_trade_normal.type AS type,
                    instance_trade_normal.population AS afterPopulation,
                    instance_trade_normal.gdp AS afterGdp,
                    instance_trade_normal.netMT AS afterNetMT
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
                    instance_trade_normal.netMT AS beforeNetMT
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
            (afterGdp / afterPopulation - beforeGdp / beforePopulation) / (beforeGdp / beforePopulation) AS gdpPerCapChange,
            (afterNetMT - beforeNetMT) / beforeNetMT AS netMTChange,
            after.region AS region,
            after.type AS type,
            after.afterGdp AS afterGdp,
            after.afterPopulation AS afterPopulation,
            before.beforeNetMT AS beforeNetMT,
            after.afterNetMT AS afterNetMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear
        FROM
            (
                SELECT
                    instance_trade_normal.year AS afterYear,
                    instance_trade_normal.region AS region,
                    instance_trade_normal.type AS type,
                    instance_trade_normal.population AS afterPopulation,
                    instance_trade_normal.gdp AS afterGdp,
                    instance_trade_normal.netMT AS afterNetMT
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
                    instance_trade_normal.netMT AS beforeNetMT
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
            (afterGdp / afterPopulation - beforeGdp / beforePopulation) / (beforeGdp / beforePopulation) AS gdpPerCapChange,
            (afterNetMT - beforeNetMT) / beforeNetMT AS netMTChange,
            after.region AS region,
            after.type AS type,
            after.afterGdp AS afterGdp,
            after.afterPopulation AS afterPopulation,
            before.beforeNetMT AS beforeNetMT,
            after.afterNetMT AS afterNetMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear
        FROM
            (
                SELECT
                    instance_trade_normal.year AS afterYear,
                    instance_trade_normal.region AS region,
                    instance_trade_normal.type AS type,
                    instance_trade_normal.population AS afterPopulation,
                    instance_trade_normal.gdp AS afterGdp,
                    instance_trade_normal.netMT AS afterNetMT
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
                    instance_trade_normal.netMT AS beforeNetMT
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
            (afterGdp / afterPopulation - beforeGdp / beforePopulation) / (beforeGdp / beforePopulation) AS gdpPerCapChange,
            (afterNetMT - beforeNetMT) / beforeNetMT AS netMTChange,
            after.region AS region,
            after.type AS type,
            after.afterGdp AS afterGdp,
            after.afterPopulation AS afterPopulation,
            before.beforeNetMT AS beforeNetMT,
            after.afterNetMT AS afterNetMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear
        FROM
            (
                SELECT
                    instance_trade_normal.year AS afterYear,
                    instance_trade_normal.region AS region,
                    instance_trade_normal.type AS type,
                    instance_trade_normal.population AS afterPopulation,
                    instance_trade_normal.gdp AS afterGdp,
                    instance_trade_normal.netMT AS afterNetMT
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
                    instance_trade_normal.netMT AS beforeNetMT
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
            (afterGdp / afterPopulation - beforeGdp / beforePopulation) / (beforeGdp / beforePopulation) AS gdpPerCapChange,
            (afterNetMT - beforeNetMT) / beforeNetMT AS netMTChange,
            after.region AS region,
            after.type AS type,
            after.afterGdp AS afterGdp,
            after.afterPopulation AS afterPopulation,
            before.beforeNetMT AS beforeNetMT,
            after.afterNetMT AS afterNetMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear
        FROM
            (
                SELECT
                    instance_trade_normal.year AS afterYear,
                    instance_trade_normal.region AS region,
                    instance_trade_normal.type AS type,
                    instance_trade_normal.population AS afterPopulation,
                    instance_trade_normal.gdp AS afterGdp,
                    instance_trade_normal.netMT AS afterNetMT
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
                    instance_trade_normal.netMT AS beforeNetMT
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
            (afterGdp / afterPopulation - beforeGdp / beforePopulation) / (beforeGdp / beforePopulation) AS gdpPerCapChange,
            (afterNetMT - beforeNetMT) / beforeNetMT AS netMTChange,
            after.region AS region,
            after.type AS type,
            after.afterGdp AS afterGdp,
            after.afterPopulation AS afterPopulation,
            before.beforeNetMT AS beforeNetMT,
            after.afterNetMT AS afterNetMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear
        FROM
            (
                SELECT
                    instance_trade_normal.year AS afterYear,
                    instance_trade_normal.region AS region,
                    instance_trade_normal.type AS type,
                    instance_trade_normal.population AS afterPopulation,
                    instance_trade_normal.gdp AS afterGdp,
                    instance_trade_normal.netMT AS afterNetMT
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
                    instance_trade_normal.netMT AS beforeNetMT
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
            (afterGdp / afterPopulation - beforeGdp / beforePopulation) / (beforeGdp / beforePopulation) AS gdpPerCapChange,
            (afterNetMT - beforeNetMT) / beforeNetMT AS netMTChange,
            after.region AS region,
            after.type AS type,
            after.afterGdp AS afterGdp,
            after.afterPopulation AS afterPopulation,
            before.beforeNetMT AS beforeNetMT,
            after.afterNetMT AS afterNetMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear
        FROM
            (
                SELECT
                    instance_trade_normal.year AS afterYear,
                    instance_trade_normal.region AS region,
                    instance_trade_normal.type AS type,
                    instance_trade_normal.population AS afterPopulation,
                    instance_trade_normal.gdp AS afterGdp,
                    instance_trade_normal.netMT AS afterNetMT
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
                    instance_trade_normal.netMT AS beforeNetMT
                FROM
                    instance_trade_normal
            ) before
        ON
            after.afterYear = before.beforeYear - 5
            AND after.region = before.region
            AND after.type = before.type
    ) unioned