UPDATE
    {{table_name}}
SET
    eolLandfillPercent = (
        CASE
            WHEN incinerationIncrease > 0 THEN eolLandfillPercent - incinerationIncrease
            ELSE eolLandfillPercent
        END
    ),
    eolIncinerationPercent = (
        CASE
            WHEN incinerationIncrease > 0 THEN eolIncinerationPercent + incinerationIncrease
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
                    WHEN year < 2025 THEN 0.07 * year - 141.4
                    WHEN year >= 2025 THEN 0.07 * 2025 - 141.4
                END
            ) * eolIncinerationPercent AS incinerationIncrease
        FROM
            {{table_name}}
        WHERE
            year > 2020
            AND region = 'china'
    ) updated
WHERE
    (updated.year = {{table_name}}.year)
    AND updated.region = {{table_name}}.region