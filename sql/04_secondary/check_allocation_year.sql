SELECT
    count(1) AS countInvalid
FROM
    (
        SELECT
            expected.year AS year,
            abs(expected.totalRecycledMT - actual.totalRecycledMT) / expected.totalRecycledMT AS percentDelta,
            expected.totalRecycledMT AS expectedTotalRecycledMT,
            actual.totalRecycledMT AS actualTotalRecycledMT
        FROM
            (
                SELECT
                    year,
                    sum(newWasteMT * eolRecyclingPercent * 0.8) AS totalRecycledMT
                FROM
                    consumption_intermediate_waste
                GROUP BY
                    year
            ) expected
        INNER JOIN
            (
                SELECT
                    year,
                    SUM(
                        consumptionAgricultureMT +
                        consumptionConstructionMT +
                        consumptionElectronicMT +
                        consumptionHouseholdLeisureSportsMT +
                        consumptionOtherMT +
                        consumptionPackagingMT +
                        consumptionTextileMT +
                        consumptionTransportationMT
                    ) AS totalRecycledMT
                FROM
                    consumption_secondary_no_displace
                GROUP BY
                    year
            ) actual
        ON
            expected.year = actual.year
    ) deltas
WHERE
    deltas.percentDelta > 0.01