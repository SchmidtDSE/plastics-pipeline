CREATE VIEW raw_net_import_finished_goods AS
SELECT
    {% for region in regions %}
    CAST({{ region["sqlName"] }} AS REAL) AS {{ region["key"] }},
    {% endfor %}
    CAST(Year AS INTEGER) as year
FROM
    file_07netimportplasticinfinishedgoodsnofibercopy