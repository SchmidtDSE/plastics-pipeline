CREATE VIEW raw_eol_china AS
SELECT
    CAST(YEAR_2 AS INTEGER) AS year,
    Recycling AS recycling,
    Incineration AS incineration,
    Landfill AS landfill,
    Mismanaged AS mismanaged,
    'China' AS region
FROM
    file_14eolchinacopy