CREATE VIEW raw_net_trade_eu30 AS
SELECT
    CAST(Year AS INTEGER) AS year,
    CAST(Transportation AS REAL) AS transportation,
    CAST(Packaging AS REAL) AS packing,
    CAST(Building_Construction AS REAL) AS construction,
    CAST(Electrical_Electronic AS REAL) AS electronic,
    CAST(Household_Leisure_Sports AS REAL) AS householdLeisureSports,
    CAST(Agriculture AS REAL) AS agriculture,
    CAST(Textiles AS REAL) AS textile,
    CAST(Others AS REAL) AS other
FROM
    file_20nettradeeu