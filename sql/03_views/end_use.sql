CREATE VIEW end_use AS
{% for region in regions %}
SELECT
    majorMarketSector AS majorMarketSector,
    'LLDPE LDPE' AS type,
    '{{ region["key"] }}' AS region,
    lldpeLdpe AS percent
FROM
    raw_end_use_{{ region["key"] }}
UNION ALL
SELECT
    majorMarketSector AS majorMarketSector,
    'HDPE' AS type,
    '{{ region["key"] }}' AS region,
    hdpe AS percent
FROM
    raw_end_use_{{ region["key"] }}
UNION ALL
SELECT
    majorMarketSector AS majorMarketSector,
    'PP' AS type,
    '{{ region["key"] }}' AS region,
    pp AS percent
FROM
    raw_end_use_{{ region["key"] }}
UNION ALL
SELECT
    majorMarketSector AS majorMarketSector,
    'PS' AS type,
    '{{ region["key"] }}' AS region,
    ps AS percent
FROM
    raw_end_use_{{ region["key"] }}
UNION ALL
SELECT
    majorMarketSector AS majorMarketSector,
    'PVC' AS type,
    '{{ region["key"] }}' AS region,
    pvc AS percent
FROM
    raw_end_use_{{ region["key"] }}
UNION ALL
SELECT
    majorMarketSector AS majorMarketSector,
    'PET' AS type,
    '{{ region["key"] }}' AS region,
    pet AS percent
FROM
    raw_end_use_{{ region["key"] }}
UNION ALL
SELECT
    majorMarketSector AS majorMarketSector,
    'PUR' AS type,
    '{{ region["key"] }}' AS region,
    pur AS percent
FROM
    raw_end_use_{{ region["key"] }}
UNION ALL
SELECT
    majorMarketSector AS majorMarketSector,
    'Other Termoplastics' AS type,
    '{{ region["key"] }}' AS region,
    otherThermoplastics AS percent
FROM
    raw_end_use_{{ region["key"] }}
UNION ALL
SELECT
    majorMarketSector AS majorMarketSector,
    'Other Termosets' AS type,
    '{{ region["key"] }}' AS region,
    otherThermosets AS percent
FROM
    raw_end_use_{{ region["key"] }}
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
