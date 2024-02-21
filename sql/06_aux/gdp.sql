CREATE VIEW gdp AS
SELECT
    region AS region,
    CAST(year AS INTEGER) AS year,
    CAST(gdp AS REAL) AS gdp,
    CAST(gdpSum AS REAL) AS gdpSum
FROM
    file_gdpregions