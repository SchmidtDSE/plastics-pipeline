CREATE VIEW input_import AS
{% for region in regions %}
SELECT
    year AS year,
    '{{ region["key"] }}' AS region,
    'articles' AS type,
    {{ region["key"] }} AS amountMT
FROM
    raw_net_import_articles
UNION ALL
SELECT
    year AS year,
    '{{ region["key"] }}' AS region,
    'fiber' AS type,
    {{ region["key"] }} AS amountMT
FROM
    raw_net_import_fibers
UNION ALL
SELECT
    year AS year,
    '{{ region["key"] }}' AS region,
    'goods' AS type,
    {{ region["key"] }} AS amountMT
FROM
    raw_net_import_finished_goods
UNION ALL
SELECT
    year AS year,
    '{{ region["key"] }}' AS region,
    'resin' AS type,
    {{ region["key"] }} AS amountMT
FROM
    raw_net_import_resin
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
