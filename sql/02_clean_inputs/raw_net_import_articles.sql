CREATE VIEW raw_net_import_articles AS
SELECT
    {% for region in regions %}
    CAST({{ region["sqlName"] }} AS REAL) AS {{ region["key"] }},
    {% endfor %}
    CAST(Year AS INTEGER) as year
FROM
    file_06netimportplasticarticlescopy