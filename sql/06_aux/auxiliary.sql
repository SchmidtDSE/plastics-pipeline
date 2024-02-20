CREATE VIEW auxiliary AS
SELECT
    population.year AS year,
    population.region AS region,
    population.population AS population,
    gdp.gdp AS gdp
FROM
    population
INNER JOIN
    gdp
ON
    gdp.year = population.year
    AND gdp.region = population.region