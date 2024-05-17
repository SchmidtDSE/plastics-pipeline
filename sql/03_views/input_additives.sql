CREATE VIEW input_additives AS
{% for region in regions %}
SELECT
    year AS year,
    '{{ region["key"] }}' AS region,
    {{ region["key"] }} AS amountMT
FROM
    raw_additives
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
