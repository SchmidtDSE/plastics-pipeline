SELECT
    count(1)
FROM
    consumption
WHERE
    primaryConsumptionMT IS NULL
    OR secondaryConsumptionMT IS NULL