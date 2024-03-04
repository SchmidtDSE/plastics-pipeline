CREATE VIEW raw_additives AS
SELECT
    {% for region in regions %}
    CAST({{ region["sqlName"] }} AS REAL) AS {{ region["key"] }},
    {% endfor %}
    CAST(Year AS INTEGER) as year
FROM
    file_03productionadditives