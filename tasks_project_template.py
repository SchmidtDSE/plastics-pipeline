"""Template Methods to use a set of related models to project forward waste, trade, and consumption.

Template Methods to use a set of related models to project forward waste, trade, and consumption.
This will, for example, use all of the machine learning models to make future projections.

License:
    BSD, see LICENSE.md
"""

import itertools
import json
import os
import pickle
import sqlite3
import statistics

import luigi
import scipy.stats

import const
import tasks_curve
import tasks_ml
import tasks_sql


class PreCheckProjectTask(luigi.Task):
    """Template Method to check that models are available for projection."""
    
    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def run(self):
        """Confirm models present and available for projection."""
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
        """Get the list of models to check for availability.

        Returns:
            List of models for which availability should be checked. This is
            effectively the model filename without the pickle extension.
        """
        raise NotImplementedError('Use implementor.')


class SeedProjectionTask(tasks_sql.SqlExecuteTask):
    """Create the scaffolding table in which projections will be made."""

    def transform_sql(self, sql_contents):
        """Specify the table in which the scaffolding should be built."""
        return sql_contents.format(table_name=self.get_table_name())

    def get_scripts(self):
        """Indicate that the build model table script should be used."""
        return ['08_project/build_model_table.sql']

    def get_table_name(self):
        """Get in which table the scaffolding should be built.

        Returns:
            The name of the table where the scaffolding should be built.
        """
        raise NotImplementedError('Use implementor.')


class CheckSeedProjectionTask(tasks_sql.SqlCheckTask):
    """Check that the scaffolding for a projection table was built."""

    def get_table_name(self):
        """Get the name of the table where the scaffolding was built.

        Returns:
            The name of the table to check.
        """
        raise NotImplementedError('Use implementor.')


class ProjectRawTask(luigi.Task):
    """Use models to make initial projections in a projections table."""
    
    def run(self):
        """Load related set of models and ask them to make projections."""
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

        for year in range(2021, 2051):
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
        """Use a set of related models to make projections for a year in a region.

        Args:
            connection: The connection to the scratch SQLite database where prior data can be found
                and where the projections are to be written.
            year: The year for which projections are needed.
            region: The region for which projections are needed.
            consumption_model: DecoratedModel that will predict consumption percent change for this
                year / region.
            waste_model: DecoratedModel that will predict waste EOL fate propensity for this year /
                region.
            trade_model: DecoratedModel that will predict ratio of trade (goods and materials) to
                consumption for this year / region.
            waste_trade_model: DecoratedModel that will predict ratio of trade (waste) to
                consumption for this year / region.
        """
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

        updated_output_row = self.postprocess_row(updated_output_row)

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
                consumptionTransportationMT = {consumptionTransportationMT},
                eolRecyclingPercent = {eolRecyclingPercent},
                eolIncinerationPercent = {eolIncinerationPercent},
                eolLandfillPercent = {eolLandfillPercent},
                eolMismanagedPercent = {eolMismanagedPercent},
                netImportArticlesMT = {netImportArticlesMT},
                netImportFibersMT = {netImportFibersMT},
                netImportGoodsMT = {netImportGoodsMT},
                netImportResinMT = {netImportResinMT},
                netWasteTradeMT = {netWasteTradeMT},
                netImportArticlesPercent = {netImportArticlesPercent},
                netImportFibersPercent = {netImportFibersPercent},
                netImportGoodsPercent = {netImportGoodsPercent},
                netImportResinPercent = {netImportResinPercent},
                netWasteTradePercent = {netWasteTradePercent}
            WHERE
                year = {year}
                AND region = '{region}'
        '''.format(**updated_output_row))

        connection.commit()
        cursor.close()

    def get_model(self, filename, job_info):
        """Load a model.

        Args:
            filename: The path at which the model with its metadata were written.
            job_info: Information about the directories of the workspace being used.

        Returns:
            Loaded DecoratedModel.
        """
        model_loc = os.path.join(
            job_info['directories']['workspace'],
            filename
        )
        with open(model_loc, 'rb') as f:
            return pickle.load(f)['model']

    def build_instances(self, connection, sql, cols):
        """Build tasks / input data required to make future predictions.

        Args:
            connection: The SQLite database at which instances / input data can be found.
            sql: The query to use in order to load tasks / input data.
            cols: List of strings describing the columns to be returned from the query.

        Returns:
            List of dictionaries where each is a task for a prediction to be made with its input
            data.
        """
        cursor = connection.cursor()
        cursor.execute(sql)
        results_flat = cursor.fetchall()
        cursor.close()

        if (len(results_flat) == 0):
            raise RuntimeError('No results.')

        results_keyed = [dict(zip(cols, result)) for result in results_flat]
        return results_keyed

    def get_consumption_projections(self, connection, year, region, consumption_model):
        """Get projections for consumption percent change for all sectors in a year / region.

        Args:
            connection: Connection to the SQLite database.
            year: The year for which the projections should be made.
            region: The region for which the projections should be made.
            consumption_model: The DecoratedModel to use to make the predictions.

        Returns:
            Dictionary mapping sector to prediction after transformation.
        """
        return self.get_projections(
            connection,
            year,
            region,
            consumption_model,
            self.get_consumption_attrs(),
            lambda x: self.get_consumption_inputs_sql(year, region, x),
            lambda: self.get_consumption_inputs_cols(),
            lambda instance, prediction: self.transform_consumption_prediction(
                instance,
                prediction
            )
        )

    def get_waste_projections(self, connection, year, region, waste_model):
        """Get projections for EOL propensity for all fates in a year / region.

        Args:
            connection: Connection to the SQLite database.
            year: The year for which the projections should be made.
            region: The region for which the projections should be made.
            consumption_model: The DecoratedModel to use to make the predictions.

        Returns:
            Dictionary mapping fate to prediction after transformation.
        """
        return self.get_projections(
            connection,
            year,
            region,
            waste_model,
            self.get_waste_attrs(),
            lambda x: self.get_waste_inputs_sql(year, region, x),
            lambda: self.get_waste_inputs_cols(),
            lambda instance, prediction: self.transform_waste_prediction(
                instance,
                prediction
            )
        )

    def get_trade_projections(self, connection, year, region, trade_model):
        """Get projections for goods / materials trade for all types in a year / region.

        Args:
            connection: Connection to the SQLite database.
            year: The year for which the projections should be made.
            region: The region for which the projections should be made.
            consumption_model: The DecoratedModel to use to make the predictions.

        Returns:
            Dictionary mapping type to prediction after transformation.
        """
        return self.get_projections(
            connection,
            year,
            region,
            trade_model,
            self.get_trade_attrs(),
            lambda x: self.get_trade_inputs_sql(year, region, x),
            lambda: self.get_trade_inputs_cols(),
            lambda instance, prediction: self.transform_trade_prediction(
                instance,
                prediction
            )
        )

    def get_waste_trade_projections(self, connection, year, region, waste_trade_model):
        """Get projections for EOL propensity for all fates in a year / region.

        Args:
            connection: Connection to the SQLite database.
            year: The year for which the projections should be made.
            region: The region for which the projections should be made.
            consumption_model: The DecoratedModel to use to make the predictions.

        Returns:
            Dictionary mapping a single key to prediction after transformation.
        """
        return self.get_projections(
            connection,
            year,
            region,
            waste_trade_model,
            self.get_waste_trade_attrs(),
            lambda x: self.get_waste_trade_inputs_sql(year, region, x),
            lambda: self.get_waste_trade_inputs_cols(),
            lambda instance, prediction: self.transform_waste_trade_prediction(
                instance,
                prediction
            )
        )

    def get_projections(self, connection, year, region, model, keys, sql_getter, cols_getter,
            prediction_transformer):
        """Get the projections for a task and return results keyed by type.

        Args:
            connection: Connection to the SQLite database.
            year: The year for which the projections should be made.
            region: The region for which the projections should be made.
            model: The DecoratedModel to use to make the predictions.
            keys: The list of types for which predictions are needed.
            sql_getter: Function taking the name of a type to return the string SQL query to be used
                to request input data for that type.
            cols_getter: Function returning the ordered column names exepcted to be returned by the
                executing the query described by sql_getter.
            prediction_transformer: Function to, given the raw input value and prediction, transform
                predictions from raw values to useful predictions. This may involve calculations
                like applying a model's predicted percent change to a "prior" value.

        Returns:
            Dictionary mapping type to prediction after transformation.
        """

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
        """Get the name of the table where the predictions should be written.

        Returns:
            Table name into which this task should write.
        """
        raise NotImplementedError('Use implementor.')

    def get_consumption_model_filename(self):
        """Get the filename where the DecoratedModel for consumption prediction can be found.

        Returns:
            Model file path.
        """
        raise NotImplementedError('Use implementor.')

    def get_waste_model_filename(self):
        """Get the filename where the DecoratedModel for fate propensity prediction can be found.

        Returns:
            Model file path.
        """
        raise NotImplementedError('Use implementor.')

    def get_trade_model_filename(self):
        """Get filename where DecoratedModel for goods / materials trade prediction can be found.

        Returns:
            Model file path.
        """
        raise NotImplementedError('Use implementor.')

    def get_waste_trade_model_filename(self):
        """Get the filename where the DecoratedModel for waste trade prediction can be found.

        Returns:
            Model file path.
        """
        raise NotImplementedError('Use implementor.')

    def get_consumption_inputs_sql(self, year, region, sector):
        """Get the SQL to query for consumption model input data.

        Args:
            year: The year for which a prediction is being generated.
            region: The region for which a prediction is being generated.
            sector: The sector for which a prediction is being generated.

        Returns:
            String SQL query content.
        """
        raise NotImplementedError('Use implementor.')

    def get_waste_inputs_sql(self, year, region, type_name):
        """Get the SQL to query for EOL fate propensity model input data.

        Args:
            year: The year for which a prediction is being generated.
            region: The region for which a prediction is being generated.
            sector: The sector for which a prediction is being generated.

        Returns:
            String SQL query content.
        """
        raise NotImplementedError('Use implementor.')

    def get_trade_inputs_sql(self, year, region, type_name):
        """Get the SQL to query for goods / materials trade model input data.

        Args:
            year: The year for which a prediction is being generated.
            region: The region for which a prediction is being generated.
            sector: The sector for which a prediction is being generated.

        Returns:
            String SQL query content.
        """
        raise NotImplementedError('Use implementor.')

    def get_waste_trade_inputs_sql(self, year, region, type_name):
        """Get the SQL to query for waste trade model input data.

        Args:
            year: The year for which a prediction is being generated.
            region: The region for which a prediction is being generated.
            sector: The sector for which a prediction is being generated.

        Returns:
            String SQL query content.
        """
        raise NotImplementedError('Use implementor.')
    
    def get_consumption_inputs_cols(self):
        """Get the list of input columns expected for the consumption model.

        Returns:
            List of strings ordered as expected by the model.
        """
        raise NotImplementedError('Use implementor.')
    
    def get_waste_inputs_cols(self):
        """Get the list of input columns expected for the wate fate propensity model.

        Returns:
            List of strings ordered as expected by the model.
        """
        raise NotImplementedError('Use implementor.')
    
    def get_trade_inputs_cols(self):
        """Get the list of input columns expected for the goods / materials trade model.

        Returns:
            List of strings ordered as expected by the model.
        """
        raise NotImplementedError('Use implementor.')

    def get_waste_trade_inputs_cols(self):
        """Get the list of input columns expected for the waste trade model.

        Returns:
            List of strings ordered as expected by the model.
        """
        raise NotImplementedError('Use implementor.')

    def transform_consumption_prediction(self, instance, prediction):
        """Optional hook to transform consumption predictions prior to returning.
        
        Args:
            instance: The input data used to make the prediction.
            prediction: The raw value returned from the model.

        Returns:
            The value after transformation.
        """
        return prediction

    def transform_waste_prediction(self, instance, prediction):
        """Optional hook to transform waste fate propensity predictions prior to returning.
        
        Args:
            instance: The input data used to make the prediction.
            prediction: The raw value returned from the model.

        Returns:
            The value after transformation.
        """
        return prediction

    def transform_trade_prediction(self, instance, prediction):
        """Optional hook to transform trade (goods / materials) predictions prior to returning.
        
        Args:
            instance: The input data used to make the prediction.
            prediction: The raw value returned from the model.

        Returns:
            The value after transformation.
        """
        return prediction

    def transform_waste_trade_prediction(self, instance, prediction):
        """Optional hook to transform trade (waste) predictions prior to returning.
        
        Args:
            instance: The input data used to make the prediction.
            prediction: The raw value returned from the model.

        Returns:
            The value after transformation.
        """
        return prediction

    def get_consumption_attrs(self):
        """Get the types of consumption.

        Returns:
            List of consumption sectors.
        """
        return [
            'consumptionAgricultureMT',
            'consumptionConstructionMT',
            'consumptionElectronicMT',
            'consumptionHouseholdLeisureSportsMT',
            'consumptionOtherMT',
            'consumptionPackagingMT',
            'consumptionTextileMT',
            'consumptionTransportationMT'
        ]

    def get_waste_attrs(self):
        """Get the types of waste.

        Returns:
            List of waste fates.
        """
        return [
            'eolRecyclingPercent',
            'eolIncinerationPercent',
            'eolLandfillPercent',
            'eolMismanagedPercent'
        ]

    def get_trade_attrs(self):
        """Get the types of goods / materials trade.

        Returns:
            List of goods / materials trade types.
        """
        return [
            'netImportArticlesMT',
            'netImportFibersMT',
            'netImportGoodsMT',
            'netImportResinMT'
        ]

    def get_waste_trade_attrs(self):
        """Get the types of waste trade.

        Returns:
            List of waste trade types.
        """
        return [
            'netWasteTradeMT'
        ]

    def postprocess_row(self, target):
        """Final tasks to preprocess an output prediction row before writing to database.

        Args:
            target: The record produced.

        Returns:
            The record to be written.
        """
        total_consumption = sum(map(
            lambda x: target[x],
            [
                'consumptionAgricultureMT',
                'consumptionConstructionMT',
                'consumptionElectronicMT',
                'consumptionHouseholdLeisureSportsMT',
                'consumptionOtherMT',
                'consumptionPackagingMT',
                'consumptionTextileMT',
                'consumptionTransportationMT'
            ]
        ))
        target.update({
            'netImportArticlesPercent': target['netImportArticlesMT'] / total_consumption,
            'netImportFibersPercent': target['netImportFibersMT'] / total_consumption,
            'netImportGoodsPercent': target['netImportGoodsMT'] / total_consumption,
            'netImportResinPercent': target['netImportResinMT'] / total_consumption,
            'netWasteTradePercent': target['netWasteTradeMT'] / total_consumption
        })
        return target


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
            '08_project/normalize_eol.sql',  # Require that EOL propensity adds up to 1
            '08_project/normalize_trade.sql',  # Require imports == exports
            '08_project/normalize_waste_trade.sql',  # Require imports == exports
            '08_project/apply_china_policy_eol.sql',  # Known policy to include in BAU
            '08_project/apply_eu_policy_eol.sql'  # Known policy to include in BAU
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
            '08_project/apply_waste_trade.sql'
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

        table = self.get_table_name()

        years = list(range(1951, 2051))
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
            assert immediate > 0
            timeseries[region][year] += immediate
            total_added += immediate

            for future_year in range(year, 2051):
                year_offset = future_year - year
                percent_prior = time_distribution.cdf(year_offset - 0.5)
                percent_till_year = time_distribution.cdf(year_offset + 0.5)
                percent = percent_till_year - percent_prior
                assert percent >= 0
                amount = future_waste * percent
                timeseries[region][future_year] += amount
                total_added += amount

            if year < 2030 and sector != 'consumptionConstructionMT':
                assert abs(total_added - future_waste) < 1

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
