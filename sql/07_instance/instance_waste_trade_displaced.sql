CREATE VIEW instance_waste_trade_displaced AS
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
    unioned.afterNetMT AS afterNetMT,
    unioned.beforeYear AS beforeYear,
    unioned.afterYear AS afterYear
FROM
    (
        SELECT
            abs(after.afterYear - before.beforeYear) AS years,
            (afterPopulation - beforePopulation) / beforePopulation AS popChange,
            (afterGdp / afterPopulation - beforeGdp / beforePopulation) / (beforeGdp / beforePopulation) AS gdpPerCapChange,
            afterNetMT - beforeNetMT AS netMTChange,
            after.region AS region,
            after.afterGdp AS afterGdp,
            after.afterPopulation AS afterPopulation,
            before.beforeNetMT AS beforeNetMT,
            after.afterNetMT AS afterNetMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear
        FROM
            (
                SELECT
                    instance_waste_trade_normal.year AS afterYear,
                    instance_waste_trade_normal.region AS region,
                    instance_waste_trade_normal.population AS afterPopulation,
                    instance_waste_trade_normal.gdp AS afterGdp,
                    instance_waste_trade_normal.netMT AS afterNetMT
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
                    instance_waste_trade_normal.netMT AS beforeNetMT
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
            (afterGdp / afterPopulation - beforeGdp / beforePopulation) / (beforeGdp / beforePopulation) AS gdpPerCapChange,
            afterNetMT - beforeNetMT AS netMTChange,
            after.region AS region,
            after.afterGdp AS afterGdp,
            after.afterPopulation AS afterPopulation,
            before.beforeNetMT AS beforeNetMT,
            after.afterNetMT AS afterNetMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear
        FROM
            (
                SELECT
                    instance_waste_trade_normal.year AS afterYear,
                    instance_waste_trade_normal.region AS region,
                    instance_waste_trade_normal.population AS afterPopulation,
                    instance_waste_trade_normal.gdp AS afterGdp,
                    instance_waste_trade_normal.netMT AS afterNetMT
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
                    instance_waste_trade_normal.netMT AS beforeNetMT
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
            (afterGdp / afterPopulation - beforeGdp / beforePopulation) / (beforeGdp / beforePopulation) AS gdpPerCapChange,
            afterNetMT - beforeNetMT AS netMTChange,
            after.region AS region,
            after.afterGdp AS afterGdp,
            after.afterPopulation AS afterPopulation,
            before.beforeNetMT AS beforeNetMT,
            after.afterNetMT AS afterNetMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear
        FROM
            (
                SELECT
                    instance_waste_trade_normal.year AS afterYear,
                    instance_waste_trade_normal.region AS region,
                    instance_waste_trade_normal.population AS afterPopulation,
                    instance_waste_trade_normal.gdp AS afterGdp,
                    instance_waste_trade_normal.netMT AS afterNetMT
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
                    instance_waste_trade_normal.netMT AS beforeNetMT
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
            (afterGdp / afterPopulation - beforeGdp / beforePopulation) / (beforeGdp / beforePopulation) AS gdpPerCapChange,
            afterNetMT - beforeNetMT AS netMTChange,
            after.region AS region,
            after.afterGdp AS afterGdp,
            after.afterPopulation AS afterPopulation,
            before.beforeNetMT AS beforeNetMT,
            after.afterNetMT AS afterNetMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear
        FROM
            (
                SELECT
                    instance_waste_trade_normal.year AS afterYear,
                    instance_waste_trade_normal.region AS region,
                    instance_waste_trade_normal.population AS afterPopulation,
                    instance_waste_trade_normal.gdp AS afterGdp,
                    instance_waste_trade_normal.netMT AS afterNetMT
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
                    instance_waste_trade_normal.netMT AS beforeNetMT
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
            (afterGdp / afterPopulation - beforeGdp / beforePopulation) / (beforeGdp / beforePopulation) AS gdpPerCapChange,
            afterNetMT - beforeNetMT AS netMTChange,
            after.region AS region,
            after.afterGdp AS afterGdp,
            after.afterPopulation AS afterPopulation,
            before.beforeNetMT AS beforeNetMT,
            after.afterNetMT AS afterNetMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear
        FROM
            (
                SELECT
                    instance_waste_trade_normal.year AS afterYear,
                    instance_waste_trade_normal.region AS region,
                    instance_waste_trade_normal.population AS afterPopulation,
                    instance_waste_trade_normal.gdp AS afterGdp,
                    instance_waste_trade_normal.netMT AS afterNetMT
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
                    instance_waste_trade_normal.netMT AS beforeNetMT
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
            (afterGdp / afterPopulation - beforeGdp / beforePopulation) / (beforeGdp / beforePopulation) AS gdpPerCapChange,
            afterNetMT - beforeNetMT AS netMTChange,
            after.region AS region,
            after.afterGdp AS afterGdp,
            after.afterPopulation AS afterPopulation,
            before.beforeNetMT AS beforeNetMT,
            after.afterNetMT AS afterNetMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear
        FROM
            (
                SELECT
                    instance_waste_trade_normal.year AS afterYear,
                    instance_waste_trade_normal.region AS region,
                    instance_waste_trade_normal.population AS afterPopulation,
                    instance_waste_trade_normal.gdp AS afterGdp,
                    instance_waste_trade_normal.netMT AS afterNetMT
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
                    instance_waste_trade_normal.netMT AS beforeNetMT
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
            (afterGdp / afterPopulation - beforeGdp / beforePopulation) / (beforeGdp / beforePopulation) AS gdpPerCapChange,
            afterNetMT - beforeNetMT AS netMTChange,
            after.region AS region,
            after.afterGdp AS afterGdp,
            after.afterPopulation AS afterPopulation,
            before.beforeNetMT AS beforeNetMT,
            after.afterNetMT AS afterNetMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear
        FROM
            (
                SELECT
                    instance_waste_trade_normal.year AS afterYear,
                    instance_waste_trade_normal.region AS region,
                    instance_waste_trade_normal.population AS afterPopulation,
                    instance_waste_trade_normal.gdp AS afterGdp,
                    instance_waste_trade_normal.netMT AS afterNetMT
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
                    instance_waste_trade_normal.netMT AS beforeNetMT
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
            (afterGdp / afterPopulation - beforeGdp / beforePopulation) / (beforeGdp / beforePopulation) AS gdpPerCapChange,
            afterNetMT - beforeNetMT AS netMTChange,
            after.region AS region,
            after.afterGdp AS afterGdp,
            after.afterPopulation AS afterPopulation,
            before.beforeNetMT AS beforeNetMT,
            after.afterNetMT AS afterNetMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear
        FROM
            (
                SELECT
                    instance_waste_trade_normal.year AS afterYear,
                    instance_waste_trade_normal.region AS region,
                    instance_waste_trade_normal.population AS afterPopulation,
                    instance_waste_trade_normal.gdp AS afterGdp,
                    instance_waste_trade_normal.netMT AS afterNetMT
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
                    instance_waste_trade_normal.netMT AS beforeNetMT
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
            (afterGdp / afterPopulation - beforeGdp / beforePopulation) / (beforeGdp / beforePopulation) AS gdpPerCapChange,
            afterNetMT - beforeNetMT AS netMTChange,
            after.region AS region,
            after.afterGdp AS afterGdp,
            after.afterPopulation AS afterPopulation,
            before.beforeNetMT AS beforeNetMT,
            after.afterNetMT AS afterNetMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear
        FROM
            (
                SELECT
                    instance_waste_trade_normal.year AS afterYear,
                    instance_waste_trade_normal.region AS region,
                    instance_waste_trade_normal.population AS afterPopulation,
                    instance_waste_trade_normal.gdp AS afterGdp,
                    instance_waste_trade_normal.netMT AS afterNetMT
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
                    instance_waste_trade_normal.netMT AS beforeNetMT
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
            (afterGdp / afterPopulation - beforeGdp / beforePopulation) / (beforeGdp / beforePopulation) AS gdpPerCapChange,
            afterNetMT - beforeNetMT AS netMTChange,
            after.region AS region,
            after.afterGdp AS afterGdp,
            after.afterPopulation AS afterPopulation,
            before.beforeNetMT AS beforeNetMT,
            after.afterNetMT AS afterNetMT,
            before.beforeYear AS beforeYear,
            after.afterYear AS afterYear
        FROM
            (
                SELECT
                    instance_waste_trade_normal.year AS afterYear,
                    instance_waste_trade_normal.region AS region,
                    instance_waste_trade_normal.population AS afterPopulation,
                    instance_waste_trade_normal.gdp AS afterGdp,
                    instance_waste_trade_normal.netMT AS afterNetMT
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
                    instance_waste_trade_normal.netMT AS beforeNetMT
                FROM
                    instance_waste_trade_normal
            ) before
        ON
            after.afterYear = before.beforeYear - 5
            AND after.region = before.region
    ) unioned