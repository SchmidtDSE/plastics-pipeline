"""Tasks for applying lifecycle distributions and normalizing values.

License:
    BSD, see LICENSE.md
"""

import json
import math
import sqlite3

import luigi
import scipy.stats

import const
import tasks_sql


class NormalizeProjectionTask(tasks_sql.SqlExecuteTask):
    """Enforce certain constraints such that all numbers of a type add up to 1.

    Enforce certain constraints such that all numbers of a type add up to 1. This may also be used
    to apply policies which are recent so may not be reflected in machine learning but which are
    part of the "business as usual" scenario.
    """

    def transform_sql(self, sql_contents):
        """Transform a SQL query to target a specific table."""
        return sql_contents.format(table_name=self.get_table_name())

    def get_scripts(self):
        """Get the scripts which perform normalization."""
        return [
            '09_project/normalize_eol.sql',  # Require that EOL propensity adds up to 1
            '09_project/normalize_trade.sql',  # Require imports == exports
            '09_project/normalize_waste_trade.sql',  # Require imports == exports
            '09_project/apply_china_policy_eol.sql',  # Known policy to include in BAU
            '09_project/apply_eu_policy_eol.sql'  # Known policy to include in BAU
        ]

    def get_table_name(self):
        """Get the name of the table in which to perform normalization.

        Returns:
            The name of the table to normalize.
        """
        raise NotImplementedError('Use implementor.')


class ApplyWasteTradeProjectionTask(tasks_sql.SqlExecuteTask):
    """Apply waste trade to summary statistics."""

    def transform_sql(self, sql_contents):
        """Transform a SQL query to target a specific table."""
        return sql_contents.format(table_name=self.get_table_name())

    def get_scripts(self):
        """Get the scripts which perform tje calculation."""
        return [
            '09_project/apply_waste_trade.sql'
        ]

    def get_table_name(self):
        """Get the name of the table in which to perform the calculation.

        Returns:
            The name of the table in which to perform the calculation.
        """
        raise NotImplementedError('Use implementor.')


class NormalizeCheckTask(luigi.Task):
    """Task to check that normalization was successful."""

    def get_table_name(self):
        """Get the name of the table to check.

        Returns:
            The name of the table to check.
        """
        raise NotImplementedError('Must use implementor.')

    def run(self):
        """Perform normalization checks."""
        with self.input().open('r') as f:
            job_info = json.load(f)

        database_loc = job_info['database']
        connection = sqlite3.connect(database_loc)
        cursor = connection.cursor()

        table = self.get_table_name()

        cursor.execute('SELECT count(1) FROM {table}'.format(table=table))
        results = cursor.fetchall()
        assert results[0][0] > 0

        cursor.execute('''
            SELECT
                count(1)
            FROM
                (
                    SELECT
                        year,
                        sum(netImportArticlesMT) AS totalArticlesMT,
                        sum(netImportFibersMT) AS totalFibersMT,
                        sum(netImportGoodsMT) AS totalGoodsMT,
                        sum(netImportResinMT) AS totalResinMT
                    FROM
                        {table}
                    GROUP BY
                        year
                ) global_vals
            WHERE
                (
                    abs(global_vals.totalArticlesMT) > 0.0001
                    OR abs(global_vals.totalFibersMT) > 0.0001
                    OR abs(global_vals.totalGoodsMT) > 0.0001
                    OR abs(global_vals.totalResinMT) > 0.0001
                )
                AND year > 2021
        '''.format(table=table))
        results = cursor.fetchall()
        assert results[0][0] == 0

        cursor.execute('''
            SELECT
                count(1)
            FROM
                (
                    SELECT
                        year,
                        sum(netWasteTradeMT) AS netWasteTradeMT
                    FROM
                        {table}
                    GROUP BY
                        year
                ) global_vals
            WHERE
                (
                    abs(global_vals.netWasteTradeMT) > 0.0001
                )
                AND year > 2021
        '''.format(table=table))
        results = cursor.fetchall()
        assert results[0][0] == 0

        if self.should_assert_trade_max():
            cursor.execute('''
                SELECT
                    count(1)
                FROM
                    (
                        SELECT
                            year,
                            sum(abs(netWasteTradeMT)) AS netWasteTradeMT
                        FROM
                            {table}
                        GROUP BY
                            year
                    ) global_vals
                WHERE
                    (
                        abs(global_vals.netWasteTradeMT) < 0.5
                    )
                    AND year > 2021
            '''.format(table=table))
            results = cursor.fetchall()
            assert results[0][0] == 0

        if self.should_assert_waste_trade_min():
            cursor.execute('''
                SELECT
                    count(1)
                FROM
                    (
                        SELECT
                            year,
                            region,
                            abs(
                                (
                                    netImportArticlesMT +
                                    netImportFibersMT +
                                    netImportGoodsMT
                                ) / (
                                    consumptionAgricultureMT +
                                    consumptionConstructionMT +
                                    consumptionElectronicMT +
                                    consumptionHouseholdLeisureSportsMT +
                                    consumptionPackagingMT +
                                    consumptionTransportationMT +
                                    consumptionTextileMT +
                                    consumptionOtherMT
                                )
                            ) * 100 AS percentTrade
                        FROM
                            {table}
                    ) global_vals
                WHERE
                    percentTrade > 50
                    AND year > 2021
            '''.format(table=table))
            results = cursor.fetchall()
            assert results[0][0] == 0

        cursor.execute('''
            SELECT
                count(1)
            FROM
                (
                    SELECT
                        year,
                        (
                            eolRecyclingPercent +
                            eolIncinerationPercent +
                            eolLandfillPercent +
                            eolMismanagedPercent
                        ) AS totalEolShare
                    FROM
                        {table}
                ) global_vals
            WHERE
                abs(totalEolShare - 1) > 0.001
                AND global_vals.year > 2021
        '''.format(table=table))
        results = cursor.fetchall()
        assert results[0][0] == 0

        connection.commit()

        cursor.close()
        connection.close()

        with self.output().open('w') as f:
            return json.dump(job_info, f)

    def should_assert_waste_trade_min(self):
        """Indicate if a nominal minmum waste trade should be asserted.

        Some degree of waste trade is still expected in the future and enabling this assertion asks
        this task to enforce a nominal minimum waste trade.

        Returns:
            True if it should be enforced and False if any waste trade level should be allowed.
        """
        return False

    def should_assert_trade_max(self):
        """Indicate if a nominal maximum goods and materials trade should be asserted.

        Trade should not overwhelm all domestic production and enabling this assertion asks this
        task to enforce a nominal maximum goods and materials trade.

        Returns:
            True if it should be enforced and False if any waste trade level should be allowed.
        """
        return False


class ApplyLifecycleTask(luigi.Task):
    """Task which determines waste through lifecycle distributions.

    Task which determines when goods will become waste based on "lifetime" distributions, updating
    the projections table in the process.
    """

    def get_table_name(self):
        """Get the name of the table to update.

        Returns:
            The name of the table to update.
        """
        raise NotImplementedError('Must use implementor.')

    def run(self):
        """Make waste projections."""
        with self.input().open('r') as f:
            job_info = json.load(f)

        database_loc = job_info['database']
        connection = sqlite3.connect(database_loc)

        years = list(range(self.get_start_year(), self.get_end_year() + 1))
        regions = ['china', 'eu30', 'nafta', 'row']

        timeseries = dict(map(
            lambda region: (
                region,
                dict(map(lambda year: (year, 0), years))
            ),
            regions
        ))

        for year in years:
            for region in regions:
                self.add_to_timeseries(connection, timeseries, region, year)

        self.update_waste_timeseries(connection, timeseries)

        connection.close()

        with self.output().open('w') as f:
            return json.dump(job_info, f)

    def add_to_timeseries(self, connection, timeseries, region, year):
        """Make waste predictions.

        Args:
            connection: Connection to the SQLite database where consumption can be found.
            timeseries: Dict inside dict where timeseries[region][year] is the amount of waste
                expected for a region / year. This is where predictions should be written. This will
                be modified in place.
            region: The region for which predictions should be made.
            year: The year for which predictions should be made.
        """
        sql = '''
            SELECT
                consumptionAgricultureMT,
                consumptionConstructionMT,
                consumptionElectronicMT,
                consumptionHouseholdLeisureSportsMT,
                consumptionPackagingMT,
                consumptionTransportationMT,
                consumptionTextileMT,
                consumptionOtherMT
            FROM
                {table_name}
            WHERE
                year = {year}
                AND region = '{region}'
        '''.format(year=year, region=region, table_name=self.get_table_name())

        sectors = [
            'consumptionAgricultureMT',
            'consumptionConstructionMT',
            'consumptionElectronicMT',
            'consumptionHouseholdLeisureSportsMT',
            'consumptionPackagingMT',
            'consumptionTransportationMT',
            'consumptionTextileMT',
            'consumptionOtherMT'
        ]

        cursor = connection.cursor()
        cursor.execute(sql)

        results = cursor.fetchall()
        assert len(results) == 1

        cursor.close()

        result_flat = results[0]
        result = dict(zip(sectors, result_flat))

        for sector in sectors:
            future_waste = result[sector]

            if future_waste is None or future_waste < 0:
                future_waste = 0

            distribution = const.LIFECYCLE_DISTRIBUTIONS[sector]
            sigma = distribution['sigma']
            mu = distribution['mu']
            time_distribution = scipy.stats.lognorm(s=sigma, scale=math.exp(mu))

            total_added = 0

            year_offset = 0
            immediate = time_distribution.cdf(year_offset - 0.5) * future_waste
            assert immediate >= 0
            timeseries[region][year] += immediate
            total_added += immediate

            for future_year in range(year, self.get_end_year() + 1):
                year_offset = future_year - year
                percent_prior = time_distribution.cdf(year_offset - 0.5)
                percent_till_year = time_distribution.cdf(year_offset + 0.5)
                percent = percent_till_year - percent_prior
                assert percent >= 0
                amount = future_waste * percent
                timeseries[region][future_year] += amount
                total_added += amount

            year_in_range = year < self.get_end_assert_year()
            sector_allowed = sector != 'consumptionConstructionMT'
            has_future_waste = future_waste > 0
            if year_in_range and sector_allowed and has_future_waste:
                percent_waiting = abs(total_added - future_waste) / future_waste
                assert percent_waiting < self.get_allowed_waste_waiting()

    def update_waste_timeseries(self, connection, timeseries):
        """Persist waste predictions to the database.

        Args:
            connection: The SQLite database where predictions should be written.
            timeseries: Dict inside dict where timeseries[region][year] is the waste prediction for
                a region / year combination. This is the dataset to persist.
        """
        cursor = connection.cursor()

        for region_name, region_timeseries in timeseries.items():
            for year, year_value in region_timeseries.items():
                sql = '''
                    UPDATE
                        {table_name}
                    SET
                        newWasteMT = {value}
                    WHERE
                        year = {year}
                        AND region = '{region}'
                '''.format(
                    year=year,
                    region=region_name,
                    table_name=self.get_table_name(),
                    value=year_value
                )
                cursor.execute(sql)

        cursor.close()
        connection.commit()

    def get_start_year(self):
        return 1951
    
    def get_end_assert_year(self):
        return 2030
    
    def get_allowed_waste_waiting(self):
        return 0.01

    def get_end_year(self):
        return 2050


class LifecycleCheckTask(luigi.Task):
    """Check that the lifecycle distributions were applied."""

    def get_table_name(self):
        """Get the name of the table to check.

        Returns:
            The name of the table to check.
        """
        raise NotImplementedError('Must use implementor.')

    def run(self):
        """Check that the lifecycle distribution was applied."""
        with self.input().open('r') as f:
            job_info = json.load(f)

        database_loc = job_info['database']
        connection = sqlite3.connect(database_loc)
        cursor = connection.cursor()

        table = self.get_table_name()

        cursor.execute('SELECT count(1) FROM {table}'.format(table=table))
        results = cursor.fetchall()
        assert results[0][0] > 0

        cursor.execute('''
            SELECT
                count(1)
            FROM
                {table}
            WHERE
                newWasteMT <= 0
        '''.format(table=table))
        results = cursor.fetchall()
        assert results[0][0] == 0

        connection.commit()

        cursor.close()
        connection.close()

        with self.output().open('w') as f:
            return json.dump(job_info, f)
