CREATE VIEW overview_end_use AS
{% for region in regions %}
SELECT
    majorMarketSector AS majorMarketSector,
    '{{ region["key"] }}' AS region,
    total AS percent
FROM
    raw_end_use_{{ region["key"] }}
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
