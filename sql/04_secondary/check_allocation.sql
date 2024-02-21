SELECT
	count(1) AS countInvalid
FROM
	(
		SELECT
			expected.region AS region,
			expected.year AS year,
			abs(expected.totalRecycledMT - actual.totalRecycledMT) / expected.totalRecycledMT AS percentDelta,
			expected.totalRecycledMT AS expectedTotalRecycledMT,
			actual.totalRecycledMT AS actualTotalRecycledMT
		FROM
			(
				SELECT
					region,
					year,
					newWasteMT * eolRecyclingPercent * 0.8 AS totalRecycledMT
				FROM
					consumption_intermediate_waste
			) expected
		INNER JOIN
			(
				SELECT
					region,
					year,
					(
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
					consumption_secondary_trade_pending
			) actual
		ON
			expected.region = actual.region
			AND expected.year = actual.year
	) deltas
WHERE
	deltas.percentDelta > 0.01