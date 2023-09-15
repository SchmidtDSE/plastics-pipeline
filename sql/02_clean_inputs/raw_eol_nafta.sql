CREATE VIEW raw_eol_nafta AS
SELECT
    CAST(YEAR_2 AS INTEGER) AS year,
    Recycling AS recycling,
    Incineration AS incineration,
    Landfill AS landfill,
    Mismanaged AS mismanaged,
    'NAFTA' AS region
FROM
    file_16eolnaftacopy