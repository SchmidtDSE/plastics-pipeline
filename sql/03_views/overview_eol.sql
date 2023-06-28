CREATE VIEW overview_eol AS
SELECT
    year,
    region,
    sum(
        CASE
            WHEN type = 'recycling' THEN percent
            ELSE 0
        END
    ) AS recyclingPercent,
    sum(
        CASE
            WHEN type = 'incineration' THEN percent
            ELSE 0
        END
    ) AS incinerationPercent,
    sum(
        CASE
            WHEN type = 'landfill' THEN percent
            ELSE 0
        END
    ) AS landfillPercent,
    sum(
        CASE
            WHEN type = 'mismanaged' THEN percent
            ELSE 0
        END
    ) AS mismanagedPercent
FROM
    eol
GROUP BY
    year,
    region