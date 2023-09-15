CREATE VIEW overview_sector_trade AS
SELECT
    year,
    'china' AS region,
    'Transportation' AS type,
    transportation AS netMT
FROM
    raw_net_trade_china
UNION ALL
SELECT
    year,
    'china' AS region,
    'Packaging' AS type,
    packing AS netMT
FROM
    raw_net_trade_china
UNION ALL
SELECT
    year,
    'china' AS region,
    'Building_Construction' AS type,
    construction AS netMT
FROM
    raw_net_trade_china
UNION ALL
SELECT
    year,
    'china' AS region,
    'Electrical_Electronic' AS type,
    electronic AS netMT
FROM
    raw_net_trade_china
UNION ALL
SELECT
    year,
    'china' AS region,
    'Household_Leisure_Sports' AS type,
    householdLeisureSports AS netMT
FROM
    raw_net_trade_china
UNION ALL
SELECT
    year,
    'china' AS region,
    'Agriculture' AS type,
    agriculture AS netMT
FROM
    raw_net_trade_china
UNION ALL
SELECT
    year,
    'china' AS region,
    'Textile' AS type,
    textile AS netMT
FROM
    raw_net_trade_china
UNION ALL
SELECT
    year,
    'china' AS region,
    'Others' AS type,
    other AS netMT
FROM
    raw_net_trade_china
UNION ALL
SELECT
    year,
    'eu30' AS region,
    'Transportation' AS type,
    transportation AS netMT
FROM
    raw_net_trade_eu30
UNION ALL
SELECT
    year,
    'eu30' AS region,
    'Packaging' AS type,
    packing AS netMT
FROM
    raw_net_trade_eu30
UNION ALL
SELECT
    year,
    'eu30' AS region,
    'Building_Construction' AS type,
    construction AS netMT
FROM
    raw_net_trade_eu30
UNION ALL
SELECT
    year,
    'eu30' AS region,
    'Electrical_Electronic' AS type,
    electronic AS netMT
FROM
    raw_net_trade_eu30
UNION ALL
SELECT
    year,
    'eu30' AS region,
    'Household_Leisure_Sports' AS type,
    householdLeisureSports AS netMT
FROM
    raw_net_trade_eu30
UNION ALL
SELECT
    year,
    'eu30' AS region,
    'Agriculture' AS type,
    agriculture AS netMT
FROM
    raw_net_trade_eu30
UNION ALL
SELECT
    year,
    'eu30' AS region,
    'Textile' AS type,
    textile AS netMT
FROM
    raw_net_trade_eu30
UNION ALL
SELECT
    year,
    'eu30' AS region,
    'Others' AS type,
    other AS netMT
FROM
    raw_net_trade_eu30
UNION ALL
SELECT
    year,
    'nafta' AS region,
    'Transportation' AS type,
    transportation AS netMT
FROM
    raw_net_trade_nafta
UNION ALL
SELECT
    year,
    'nafta' AS region,
    'Packaging' AS type,
    packing AS netMT
FROM
    raw_net_trade_nafta
UNION ALL
SELECT
    year,
    'nafta' AS region,
    'Building_Construction' AS type,
    construction AS netMT
FROM
    raw_net_trade_nafta
UNION ALL
SELECT
    year,
    'nafta' AS region,
    'Electrical_Electronic' AS type,
    electronic AS netMT
FROM
    raw_net_trade_nafta
UNION ALL
SELECT
    year,
    'nafta' AS region,
    'Household_Leisure_Sports' AS type,
    householdLeisureSports AS netMT
FROM
    raw_net_trade_nafta
UNION ALL
SELECT
    year,
    'nafta' AS region,
    'Agriculture' AS type,
    agriculture AS netMT
FROM
    raw_net_trade_nafta
UNION ALL
SELECT
    year,
    'nafta' AS region,
    'Textile' AS type,
    textile AS netMT
FROM
    raw_net_trade_nafta
UNION ALL
SELECT
    year,
    'nafta' AS region,
    'Others' AS type,
    other AS netMT
FROM
    raw_net_trade_nafta
UNION ALL
SELECT
    year,
    'row' AS region,
    'Transportation' AS type,
    transportation AS netMT
FROM
    raw_net_trade_row
UNION ALL
SELECT
    year,
    'row' AS region,
    'Packaging' AS type,
    packing AS netMT
FROM
    raw_net_trade_row
UNION ALL
SELECT
    year,
    'row' AS region,
    'Building_Construction' AS type,
    construction AS netMT
FROM
    raw_net_trade_row
UNION ALL
SELECT
    year,
    'row' AS region,
    'Electrical_Electronic' AS type,
    electronic AS netMT
FROM
    raw_net_trade_row
UNION ALL
SELECT
    year,
    'row' AS region,
    'Household_Leisure_Sports' AS type,
    householdLeisureSports AS netMT
FROM
    raw_net_trade_row
UNION ALL
SELECT
    year,
    'row' AS region,
    'Agriculture' AS type,
    agriculture AS netMT
FROM
    raw_net_trade_row
UNION ALL
SELECT
    year,
    'row' AS region,
    'Textile' AS type,
    textile AS netMT
FROM
    raw_net_trade_row
UNION ALL
SELECT
    year,
    'row' AS region,
    'Others' AS type,
    other AS netMT
FROM
    raw_net_trade_row