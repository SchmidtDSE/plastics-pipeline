CREATE VIEW overview_sector_trade AS
{% for region in regions %}
SELECT
    year,
    '{{ region["key"] }}' AS region,
    'Transportation' AS type,
    transportation AS netMT
FROM
    raw_net_trade_{{ region["key"] }}
UNION ALL
SELECT
    year,
    '{{ region["key"] }}' AS region,
    'Packaging' AS type,
    packing AS netMT
FROM
    raw_net_trade_{{ region["key"] }}
UNION ALL
SELECT
    year,
    '{{ region["key"] }}' AS region,
    'Building_Construction' AS type,
    construction AS netMT
FROM
    raw_net_trade_{{ region["key"] }}
UNION ALL
SELECT
    year,
    '{{ region["key"] }}' AS region,
    'Electrical_Electronic' AS type,
    electronic AS netMT
FROM
    raw_net_trade_{{ region["key"] }}
UNION ALL
SELECT
    year,
    '{{ region["key"] }}' AS region,
    'Household_Leisure_Sports' AS type,
    householdLeisureSports AS netMT
FROM
    raw_net_trade_{{ region["key"] }}
UNION ALL
SELECT
    year,
    '{{ region["key"] }}' AS region,
    'Agriculture' AS type,
    agriculture AS netMT
FROM
    raw_net_trade_{{ region["key"] }}
UNION ALL
SELECT
    year,
    '{{ region["key"] }}' AS region,
    'Textile' AS type,
    textile AS netMT
FROM
    raw_net_trade_{{ region["key"] }}
UNION ALL
SELECT
    year,
    '{{ region["key"] }}' AS region,
    'Others' AS type,
    other AS netMT
FROM
    raw_net_trade_{{ region["key"] }}
UNION ALL
{% endfor %}
