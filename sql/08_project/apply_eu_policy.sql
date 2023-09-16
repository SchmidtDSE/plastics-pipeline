UPDATE
    {table_name}
SET
    eolLandfillPercent = (
        CASE
            WHEN landfillDecrease > 0 THEN eolLandfillPercent - landfillDecrease
            ELSE eolLandfillPercent
        END
    ),
    eolIncinerationPercent = (
        CASE
            WHEN landfillDecrease > 0 THEN eolIncinerationPercent + landfillDecrease
            ELSE eolIncinerationPercent
        END
    )
FROM
    (
        SELECT
            year,
            region,
            (
                CASE
                    WHEN year < 2035 THEN eolLandfillPercent - (-0.025 * year + 50.975)
                    WHEN year >= 2035 THEN eolLandfillPercent - (-0.025 * 2035 + 50.975)
                END
            ) AS landfillDecrease
        FROM
            {table_name}
        WHERE
            year > 2020
            AND region = 'eu30'
    ) updated
WHERE
    (updated.year = {table_name}.year)
    AND updated.region = {table_name}.region