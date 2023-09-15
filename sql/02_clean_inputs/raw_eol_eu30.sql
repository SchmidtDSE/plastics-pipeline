CREATE VIEW raw_eol_eu30 AS
SELECT
    CAST(YEAR_2 AS INTEGER) AS year,
    Recycling AS recycling,
    Incineration AS incineration,
    Landfill AS landfill,
    Mismanaged AS mismanaged,
    'EU30' AS region
FROM
    file_15eoleucopy