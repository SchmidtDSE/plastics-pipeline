CREATE VIEW input_production AS
{% for region in regions %}
SELECT
    year AS year,
    '{{ region["key"] }}' AS region,
    'fiber' AS type,
    {{ region["key"] }} AS amountMT
FROM
    raw_production_fiber
UNION ALL
SELECT
    year AS year,
    '{{ region["key"] }}' AS region,
    'resin' AS type,
    {{ region["key"] }} AS amountMT
FROM
    raw_production_resin
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
