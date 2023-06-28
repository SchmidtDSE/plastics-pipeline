CREATE VIEW end_use AS
SELECT
    majorMarketSector AS majorMarketSector,
    'LLDPE LDPE' AS type,
    'china' AS region,
    lldpeLdpe AS percent
FROM
    raw_end_use_china
UNION ALL
SELECT
    majorMarketSector AS majorMarketSector,
    'HDPE' AS type,
    'china' AS region,
    hdpe AS percent
FROM
    raw_end_use_china
UNION ALL
SELECT
    majorMarketSector AS majorMarketSector,
    'PP' AS type,
    'china' AS region,
    pp AS percent
FROM
    raw_end_use_china
UNION ALL
SELECT
    majorMarketSector AS majorMarketSector,
    'PS' AS type,
    'china' AS region,
    ps AS percent
FROM
    raw_end_use_china
UNION ALL
SELECT
    majorMarketSector AS majorMarketSector,
    'PVC' AS type,
    'china' AS region,
    pvc AS percent
FROM
    raw_end_use_china
UNION ALL
SELECT
    majorMarketSector AS majorMarketSector,
    'PET' AS type,
    'china' AS region,
    pet AS percent
FROM
    raw_end_use_china
UNION ALL
SELECT
    majorMarketSector AS majorMarketSector,
    'PUR' AS type,
    'china' AS region,
    pur AS percent
FROM
    raw_end_use_china
UNION ALL
SELECT
    majorMarketSector AS majorMarketSector,
    'Other Termoplastics' AS type,
    'china' AS region,
    otherThermoplastics AS percent
FROM
    raw_end_use_china
UNION ALL
SELECT
    majorMarketSector AS majorMarketSector,
    'Other Termosets' AS type,
    'china' AS region,
    otherThermosets AS percent
FROM
    raw_end_use_china
UNION ALL
SELECT
    majorMarketSector AS majorMarketSector,
    'LLDPE LDPE' AS type,
    'eu30' AS region,
    lldpeLdpe AS percent
FROM
    raw_end_use_eu30
UNION ALL
SELECT
    majorMarketSector AS majorMarketSector,
    'HDPE' AS type,
    'eu30' AS region,
    hdpe AS percent
FROM
    raw_end_use_eu30
UNION ALL
SELECT
    majorMarketSector AS majorMarketSector,
    'PP' AS type,
    'eu30' AS region,
    pp AS percent
FROM
    raw_end_use_eu30
UNION ALL
SELECT
    majorMarketSector AS majorMarketSector,
    'PS' AS type,
    'eu30' AS region,
    ps AS percent
FROM
    raw_end_use_eu30
UNION ALL
SELECT
    majorMarketSector AS majorMarketSector,
    'PVC' AS type,
    'eu30' AS region,
    pvc AS percent
FROM
    raw_end_use_eu30
UNION ALL
SELECT
    majorMarketSector AS majorMarketSector,
    'PET' AS type,
    'eu30' AS region,
    pet AS percent
FROM
    raw_end_use_eu30
UNION ALL
SELECT
    majorMarketSector AS majorMarketSector,
    'PUR' AS type,
    'eu30' AS region,
    pur AS percent
FROM
    raw_end_use_eu30
UNION ALL
SELECT
    majorMarketSector AS majorMarketSector,
    'Other Termoplastics' AS type,
    'eu30' AS region,
    otherThermoplastics AS percent
FROM
    raw_end_use_eu30
UNION ALL
SELECT
    majorMarketSector AS majorMarketSector,
    'Other Termosets' AS type,
    'eu30' AS region,
    otherThermosets AS percent
FROM
    raw_end_use_eu30
UNION ALL
SELECT
    majorMarketSector AS majorMarketSector,
    'LLDPE LDPE' AS type,
    'nafta' AS region,
    lldpeLdpe AS percent
FROM
    raw_end_use_nafta
UNION ALL
SELECT
    majorMarketSector AS majorMarketSector,
    'HDPE' AS type,
    'nafta' AS region,
    hdpe AS percent
FROM
    raw_end_use_nafta
UNION ALL
SELECT
    majorMarketSector AS majorMarketSector,
    'PP' AS type,
    'nafta' AS region,
    pp AS percent
FROM
    raw_end_use_nafta
UNION ALL
SELECT
    majorMarketSector AS majorMarketSector,
    'PS' AS type,
    'nafta' AS region,
    ps AS percent
FROM
    raw_end_use_nafta
UNION ALL
SELECT
    majorMarketSector AS majorMarketSector,
    'PVC' AS type,
    'nafta' AS region,
    pvc AS percent
FROM
    raw_end_use_nafta
UNION ALL
SELECT
    majorMarketSector AS majorMarketSector,
    'PET' AS type,
    'nafta' AS region,
    pet AS percent
FROM
    raw_end_use_nafta
UNION ALL
SELECT
    majorMarketSector AS majorMarketSector,
    'PUR' AS type,
    'nafta' AS region,
    pur AS percent
FROM
    raw_end_use_nafta
UNION ALL
SELECT
    majorMarketSector AS majorMarketSector,
    'Other Termoplastics' AS type,
    'nafta' AS region,
    otherThermoplastics AS percent
FROM
    raw_end_use_nafta
UNION ALL
SELECT
    majorMarketSector AS majorMarketSector,
    'Other Termosets' AS type,
    'nafta' AS region,
    otherThermosets AS percent
FROM
    raw_end_use_nafta
UNION ALL
SELECT
    majorMarketSector AS majorMarketSector,
    'LLDPE LDPE' AS type,
    'row' AS region,
    lldpeLdpe AS percent
FROM
    raw_end_use_row
UNION ALL
SELECT
    majorMarketSector AS majorMarketSector,
    'HDPE' AS type,
    'row' AS region,
    hdpe AS percent
FROM
    raw_end_use_row
UNION ALL
SELECT
    majorMarketSector AS majorMarketSector,
    'PP' AS type,
    'row' AS region,
    pp AS percent
FROM
    raw_end_use_row
UNION ALL
SELECT
    majorMarketSector AS majorMarketSector,
    'PS' AS type,
    'row' AS region,
    ps AS percent
FROM
    raw_end_use_row
UNION ALL
SELECT
    majorMarketSector AS majorMarketSector,
    'PVC' AS type,
    'row' AS region,
    pvc AS percent
FROM
    raw_end_use_row
UNION ALL
SELECT
    majorMarketSector AS majorMarketSector,
    'PET' AS type,
    'row' AS region,
    pet AS percent
FROM
    raw_end_use_row
UNION ALL
SELECT
    majorMarketSector AS majorMarketSector,
    'PUR' AS type,
    'row' AS region,
    pur AS percent
FROM
    raw_end_use_row
UNION ALL
SELECT
    majorMarketSector AS majorMarketSector,
    'Other Termoplastics' AS type,
    'row' AS region,
    otherThermoplastics AS percent
FROM
    raw_end_use_row
UNION ALL
SELECT
    majorMarketSector AS majorMarketSector,
    'Other Termosets' AS type,
    'row' AS region,
    otherThermosets AS percent
FROM
    raw_end_use_row