CREATE VIEW raw_eol_row AS
SELECT
    CAST(YEAR_2 AS INTEGER) AS year,
    Recycling AS recycling,
    Incineration AS incineration,
    Landfill AS landfill,
    Mismanaged AS mismanaged,
    Region AS region
FROM
    file_17eolrowcopy