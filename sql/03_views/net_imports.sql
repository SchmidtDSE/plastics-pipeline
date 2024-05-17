CREATE VIEW net_imports AS
{% for region in regions %}
SELECT
    year,
    '{{ region["key"] }}' AS region,
    'articles' AS type,
    {{ region["key"] }} AS netMT
FROM
    raw_net_import_articles
UNION ALL
SELECT
    year,
    '{{ region["key"] }}' AS region,
    'goods' AS type,
    {{ region["key"] }} AS netMT
FROM
    raw_net_import_finished_goods
UNION ALL
SELECT
    year,
    '{{ region["key"] }}' AS region,
    'fibers' AS type,
    {{ region["key"] }} AS netMT
FROM
    raw_net_import_fibers
UNION ALL
SELECT
    year,
    '{{ region["key"] }}' AS region,
    'resin' AS type,
    {{ region["key"] }} AS netMT
FROM
    raw_net_import_resin
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
