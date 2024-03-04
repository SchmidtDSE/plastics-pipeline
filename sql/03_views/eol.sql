CREATE VIEW eol AS
{% for region in regions %}
SELECT
    year,
    '{{ region["key"] }}' AS region,
    'recycling' AS type,
    CAST(REPLACE(recycling, '%', '') AS REAL) / 100 AS percent
FROM
    raw_eol_{{ region["key"] }}
UNION ALL
SELECT
    year,
    '{{ region["key"] }}' AS region,
    'incineration' AS type,
    CAST(REPLACE(incineration, '%', '') AS REAL) / 100 AS percent
FROM
    raw_eol_{{ region["key"] }}
UNION ALL
SELECT
    year,
    '{{ region["key"] }}' AS region,
    'landfill' AS type,
    CAST(REPLACE(landfill, '%', '') AS REAL) / 100 AS percent
FROM
    raw_eol_{{ region["key"] }}
UNION ALL
SELECT
    year,
    '{{ region["key"] }}' AS region,
    'mismanaged' AS type,
    CAST(REPLACE(mismanaged, '%', '') AS REAL) / 100 AS percent
FROM
    raw_eol_{{ region["key"] }}
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
