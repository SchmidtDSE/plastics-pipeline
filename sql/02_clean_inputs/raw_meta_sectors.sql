CREATE VIEW raw_meta_sectors AS
SELECT
    majorMarketSector,
    CAST(percentage AS REAL) AS percentage
FROM
    file_rawmetasectors