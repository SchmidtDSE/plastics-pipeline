CREATE VIEW population AS
SELECT
    region AS region,
    CAST(year AS INTEGER) AS year,
    CAST(population AS REAL) AS population
FROM
    file_popregions