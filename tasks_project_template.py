import itertools
import json
import os
import pickle
import sqlite3
import statistics

import luigi

import const
import tasks_curve
import tasks_ml
import tasks_sql


class PreCheckProjectTask(luigi.Task):
    
    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def run(self):
        with self.input()[0].open('r') as f:
            job_info = json.load(f)

        models_to_check = self.get_models_to_check()

        def get_model_filename(model_name):
            return os.path.join(
                job_info['directories']['workspace'],
                model_name + '.pickle'
            )

        filenames_to_check = map(get_model_filename, models_to_check)

        for filename in filenames_to_check:
            with open(filename, 'rb') as f:
                target = pickle.load(f)
                assert 'model' in target

        with self.output().open('w') as f:
            return json.dump(job_info, f)

    def get_models_to_check(self):
        raise NotImplementedError('Use implementor.')


class SeedProjectionTask(tasks_sql.SqlExecuteTask):

    def transform_sql(self, sql_contents):
        return sql_contents.format(table_name=self.get_table_name())

    def get_scripts(self):
        return ['08_project/build_model_table.sql']

    def get_table_name(self):
        raise NotImplementedError('Use implementor.')


class CheckSeedProjectionTask(tasks_sql.SqlCheckTask):

    def get_table_name(self):
        raise NotImplementedError('Use implementor.')


class ProjectRawTask(luigi.Task):
    
    def run(self):
        with self.input().open('r') as f:
            job_info = json.load(f)

        consumption_model = self.get_model(
            self.get_consumption_model_filename(),
            job_info
        )
        waste_model = self.get_model(
            self.get_waste_model_filename(),
            job_info
        )
        trade_model = self.get_model(
            self.get_trade_model_filename(),
            job_info
        )
        waste_trade_model = self.get_model(
            self.get_waste_trade_model_filename(),
            job_info
        )

        database_loc = job_info['database']
        connection = sqlite3.connect(database_loc)

        years_before = reversed(range(1990, 2007))
        years_after = range(2021, 2051)
        years = itertools.chain(years_before, years_after)
        for year in years:
            for region in ['china', 'eu30', 'nafta', 'row']:
                self.project(
                    connection,
                    year,
                    region,
                    consumption_model,
                    waste_model,
                    trade_model,
                    waste_trade_model
                )

        connection.close()

        with self.output().open('w') as f:
            return json.dump(job_info, f)

    def project(self, connection, year, region, consumption_model, waste_model, trade_model,
        waste_trade_model):
        updated_output_row = {
            'table_name': self.get_table_name(),
            'year': year,
            'region': region
        }
        
        updated_output_row.update(self.get_consumption_projections(
            connection,
            year,
            region,
            consumption_model
        ))

        updated_output_row.update(self.get_waste_projections(
            connection,
            year,
            region,
            waste_model
        ))

        updated_output_row.update(self.get_trade_projections(
            connection,
            year,
            region,
            trade_model
        ))

        updated_output_row.update(self.get_waste_trade_projections(
            connection,
            year,
            region,
            waste_trade_model
        ))

        cursor = connection.cursor()

        cursor.execute('''
            UPDATE
                {table_name}
            SET
                consumptionAgricultureMT = {consumptionAgricultureMT},
                consumptionConstructionMT = {consumptionConstructionMT},
                consumptionElectronicMT = {consumptionElectronicMT},
                consumptionHouseholdLeisureSportsMT = {consumptionHouseholdLeisureSportsMT},
                consumptionOtherMT = {consumptionOtherMT},
                consumptionPackagingMT = {consumptionPackagingMT},
                consumptionTextileMT = {consumptionTextileMT},
                consumptionTransporationMT = {consumptionTransporationMT},
                eolRecyclingPercent = {eolRecyclingPercent},
                eolIncinerationPercent = {eolIncinerationPercent},
                eolLandfillPercent = {eolLandfillPercent},
                eolMismanagedPercent = {eolMismanagedPercent},
                netImportArticlesMT = {netImportArticlesMT},
                netImportFibersMT = {netImportFibersMT},
                netImportGoodsMT = {netImportGoodsMT},
                netImportResinMT = {netImportResinMT},
                netWasteTradeMT = {netWasteTradeMT}
            WHERE
                year = {year}
                AND region = '{region}'
        '''.format(**updated_output_row))

        connection.commit()
        cursor.close()

    def get_model(self, filename, job_info):
        model_loc = os.path.join(
            job_info['directories']['workspace'],
            filename
        )
        with open(model_loc, 'rb') as f:
            return pickle.load(f)['model']

    def build_instances(self, connection, sql, cols):
        cursor = connection.cursor()
        cursor.execute(sql)
        results_flat = cursor.fetchall()
        cursor.close()

        if (len(results_flat) == 0):
            raise RuntimeError('No results.')

        results_keyed = [dict(zip(cols, result)) for result in results_flat]
        return results_keyed

    def get_consumption_projections(self, connection, year, region, consumption_model):
        return self.get_projections(
            connection,
            year,
            region,
            consumption_model,
            [
                'consumptionAgricultureMT',
                'consumptionConstructionMT',
                'consumptionElectronicMT',
                'consumptionHouseholdLeisureSportsMT',
                'consumptionOtherMT',
                'consumptionPackagingMT',
                'consumptionTextileMT',
                'consumptionTransporationMT'
            ],
            lambda x: self.get_consumption_inputs_sql(year, region, x),
            lambda: self.get_consumption_inputs_cols(),
            lambda instance, prediction: self.transform_consumption_prediction(
                instance,
                prediction
            )
        )

    def get_waste_projections(self, connection, year, region, waste_model):
        return self.get_projections(
            connection,
            year,
            region,
            waste_model,
            [
                'eolRecyclingPercent',
                'eolIncinerationPercent',
                'eolLandfillPercent',
                'eolMismanagedPercent'
            ],
            lambda x: self.get_waste_inputs_sql(year, region, x),
            lambda: self.get_waste_inputs_cols(),
            lambda instance, prediction: self.transform_waste_prediction(
                instance,
                prediction
            )
        )

    def get_trade_projections(self, connection, year, region, trade_model):
        return self.get_projections(
            connection,
            year,
            region,
            trade_model,
            [
                'netImportArticlesMT',
                'netImportFibersMT',
                'netImportGoodsMT',
                'netImportResinMT'
            ],
            lambda x: self.get_trade_inputs_sql(year, region, x),
            lambda: self.get_trade_inputs_cols(),
            lambda instance, prediction: self.transform_trade_prediction(
                instance,
                prediction
            )
        )

    def get_waste_trade_projections(self, connection, year, region, waste_trade_model):
        return self.get_projections(
            connection,
            year,
            region,
            waste_trade_model,
            [
                'netWasteTradeMT'
            ],
            lambda x: self.get_waste_trade_inputs_sql(year, region, x),
            lambda: self.get_waste_trade_inputs_cols(),
            lambda instance, prediction: self.transform_waste_trade_prediction(
                instance,
                prediction
            )
        )

    def get_projections(self, connection, year, region, model, keys, sql_getter, cols_getter,
            prediction_transformer):

        def build_instances(label):
            sql = sql_getter(label)
            cols = cols_getter()
            return self.build_instances(connection, sql, cols)

        instances_by_key = dict(map(
            lambda x: (x, build_instances(x)),
            keys
        ))

        predictions = {}
        for key, instances in instances_by_key.items():
            raw_predictions = model.predict(instances)
            transformed_predictions = map(
                lambda x: prediction_transformer(x[0], x[1]),
                zip(instances, raw_predictions)
            )
            prediction = statistics.mean(transformed_predictions)
            predictions[key] = prediction

        return predictions

    def get_table_name(self):
        raise NotImplementedError('Use implementor.')

    def get_consumption_model_filename(self):
        raise NotImplementedError('Use implementor.')

    def get_waste_model_filename(self):
        raise NotImplementedError('Use implementor.')

    def get_trade_model_filename(self):
        raise NotImplementedError('Use implementor.')

    def get_waste_trade_model_filename(self):
        raise NotImplementedError('Use implementor.')

    def get_consumption_inputs_sql(self, year, region, sector):
        raise NotImplementedError('Use implementor.')

    def get_waste_inputs_sql(self, year, region, type_name):
        raise NotImplementedError('Use implementor.')

    def get_trade_inputs_sql(self, year, region, type_name):
        raise NotImplementedError('Use implementor.')

    def get_waste_trade_inputs_sql(self, year, region, type_name):
        raise NotImplementedError('Use implementor.')
    
    def get_consumption_inputs_cols(self):
        raise NotImplementedError('Use implementor.')
    
    def get_waste_inputs_cols(self):
        raise NotImplementedError('Use implementor.')
    
    def get_trade_inputs_cols(self):
        raise NotImplementedError('Use implementor.')

    def get_waste_trade_inputs_cols(self):
        raise NotImplementedError('Use implementor.')

    def transform_consumption_prediction(self, instance, prediction):
        raise NotImplementedError('Use implementor.')

    def transform_waste_prediction(self, instance, prediction):
        raise NotImplementedError('Use implementor.')

    def transform_trade_prediction(self, instance, prediction):
        raise NotImplementedError('Use implementor.')

    def transform_waste_trade_prediction(self, instance, prediction):
        raise NotImplementedError('Use implementor.')


class NormalizeProjectionTask(tasks_sql.SqlExecuteTask):

    def transform_sql(self, sql_contents):
        return sql_contents.format(table_name=self.get_table_name())

    def get_scripts(self):
        return [
            '08_project/normalize_eol.sql',
            '08_project/normalize_trade.sql',
            '08_project/normalize_waste_trade.sql',
            '08_project/apply_china_policy.sql',
            '08_project/apply_eu_policy.sql'
        ]

    def get_table_name(self):
        raise NotImplementedError('Use implementor.')


class ApplyWasteTradeProjectionTask(tasks_sql.SqlExecuteTask):

    def transform_sql(self, sql_contents):
        return sql_contents.format(table_name=self.get_table_name())

    def get_scripts(self):
        return [
            '08_project/apply_waste_trade.sql'
        ]

    def get_table_name(self):
        raise NotImplementedError('Use implementor.')


class NormalizeCheckTask(luigi.Task):

    def get_table_name(self):
        raise NotImplementedError('Must use implementor.')

    def run(self):
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
                AND global_vals.year > 2020
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
                AND global_vals.year > 2020
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
                            sum(abs(netWasteTradeMT)) AS netWasteTradeMT
                        FROM
                            {table}
                        GROUP BY
                            year
                    ) global_vals
                WHERE
                    (
                        abs(global_vals.netWasteTradeMT) < 2
                    )
                    AND global_vals.year > 2040
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
                AND global_vals.year > 2020
        '''.format(table=table))
        results = cursor.fetchall()
        assert results[0][0] == 0

        connection.commit()

        cursor.close()
        connection.close()

        with self.output().open('w') as f:
            return json.dump(job_info, f)

    def should_assert_waste_trade_min(self):
        return False


class ApplyLifecycleTask(luigi.Task):

    def get_table_name(self):
        raise NotImplementedError('Must use implementor.')

    def run(self):
        with self.input().open('r') as f:
            job_info = json.load(f)

        database_loc = job_info['database']
        connection = sqlite3.connect(database_loc)

        table = self.get_table_name()

        years = list(range(1990, 2051))
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
        sql = '''
            SELECT
                consumptionAgricultureMT,
                consumptionConstructionMT,
                consumptionElectronicMT,
                consumptionHouseholdLeisureSportsMT,
                consumptionPackagingMT,
                consumptionTransporationMT,
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
            'consumptionTransporationMT',
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
            
            if future_waste < 0:
                future_waste = 0

            distribution = const.LIFECYCLE_DISTRIBUTIONS[sector]
            time_distribution = statistics.NormalDist(
                mu=distribution['mean'] + year,
                sigma=distribution['std']
            )

            total_added = 0

            immediate = time_distribution.cdf(year - 0.5) * future_waste
            timeseries[region][year] += immediate
            total_added += immediate

            for future_year in range(year, 2051):
                percent_prior = time_distribution.cdf(future_year - 0.5)
                percent_till_year = time_distribution.cdf(future_year + 0.5)
                percent = percent_till_year - percent_prior
                assert percent >= 0
                amount = future_waste * percent
                timeseries[region][future_year] += amount
                total_added += amount

            if year < 2030 and sector != 'consumptionConstructionMT':
                assert abs(total_added - future_waste) < 1

    def update_waste_timeseries(self, connection, timeseries):
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


class LifecycleCheckTask(luigi.Task):

    def get_table_name(self):
        raise NotImplementedError('Must use implementor.')

    def run(self):
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
