CREATE VIEW raw_end_use_nafta AS
SELECT
    Major_Market_Sector AS majorMarketSector,
    CAST(LLDPE_LDPE AS REAL) AS lldpeLdpe,
    CAST(HDPE AS REAL) AS hdpe,
    CAST(PP AS REAL) AS pp,
    CAST(PS AS REAL) AS ps,
    CAST(PVC AS REAL) AS pvc,
    CAST(PET AS REAL) AS pet,
    CAST(PUR AS REAL) AS pur,
    CAST('Other Thermoplastics' AS REAL) AS otherThermoplastics,
    CAST('Other Thermosets' AS REAL) AS otherThermosets,
    CAST('Sum' AS REAL) AS total
FROM
    file_09naftaenduseandtype