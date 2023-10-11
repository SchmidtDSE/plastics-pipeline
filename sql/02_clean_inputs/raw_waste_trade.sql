CREATE VIEW raw_waste_trade AS
SELECT
    raw_regions.year AS year,
    (
        CASE
            WHEN raw_regions.region = 'europe' THEN 'eu30'
            ELSE raw_regions.region
        END
    ) AS region,
    raw_regions.netTons AS netTons
FROM
    (
        SELECT
            CAST(Year AS INTEGER) AS year,
            lower(RL_Class) AS region,
            CAST(Net_Waste_Trade AS REAL) AS netTons
        FROM
            file_22wastetrade
    ) raw_regions