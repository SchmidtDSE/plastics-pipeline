UPDATE
    {{table_name}}
SET
    eolRecyclingPercent = updated.eolRecyclingPercent,
    eolIncinerationPercent = updated.eolIncinerationPercent,
    eolLandfillPercent = updated.eolLandfillPercent,
    eolMismanagedPercent = updated.eolMismanagedPercent
FROM
    (
        SELECT
            year,
            region,
            eolRecyclingPercent / (
                eolRecyclingPercent +
                eolIncinerationPercent +
                eolLandfillPercent +
                eolMismanagedPercent
            ) AS eolRecyclingPercent,
            eolIncinerationPercent / (
                eolRecyclingPercent +
                eolIncinerationPercent +
                eolLandfillPercent +
                eolMismanagedPercent
            ) AS eolIncinerationPercent,
            eolLandfillPercent / (
                eolRecyclingPercent +
                eolIncinerationPercent +
                eolLandfillPercent +
                eolMismanagedPercent
            ) AS eolLandfillPercent,
            eolMismanagedPercent / (
                eolRecyclingPercent +
                eolIncinerationPercent +
                eolLandfillPercent +
                eolMismanagedPercent
            ) AS eolMismanagedPercent
        FROM
            {{table_name}}
    ) updated
WHERE
    updated.year = {{table_name}}.year
    AND updated.region = {{table_name}}.region