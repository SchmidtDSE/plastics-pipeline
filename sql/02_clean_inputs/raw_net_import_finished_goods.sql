CREATE VIEW raw_net_import_finished_goods AS
SELECT
    CAST(Year AS INTEGER) as year,
    CAST(China AS REAL) AS china,
    CAST(NAFTA AS REAL) AS nafta,
    CAST(EU30 AS REAL) AS eu30,
    CAST(RoW AS REAL) AS row
FROM
    file_07netimportplasticinfinishedgoodsnofibercopy