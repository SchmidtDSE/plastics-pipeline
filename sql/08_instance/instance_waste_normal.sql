CREATE VIEW instance_waste_normal AS
SELECT
    eol.year AS year,
    eol.region AS region,
    eol.type AS type,
    eol.percent AS percent,
    auxiliary.population AS population,
    auxiliary.gdp AS gdp
FROM
    eol
INNER JOIN
    auxiliary
ON
    eol.year = auxiliary.year
    AND eol.region = auxiliary.region