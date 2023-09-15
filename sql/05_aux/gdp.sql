CREATE VIEW gdp AS
SELECT
    region AS region,
    CAST(year AS INTEGER) AS year,
    CAST(gdp AS REAL) AS gdp
FROM
    file_gdpregions