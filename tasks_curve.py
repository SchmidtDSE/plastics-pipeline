"""Tasks which allow fitting curves to predict plastic futures.

Tasks which allow fitting curves to predict plastic futures, either by fitting without any external
variables ("naive") or using auxiliary data like GDP and population ("curve"). In practice, this
pipeline branch builds ensembles from these small models, training once per "category" of data to
be predicted like major market sector plus region or waste type plus region.

License:
    BSD, see LICENSE.md
"""

import csv
import itertools
import json
import os
import pickle
import sqlite3

import luigi
import sklearn.linear_model
import sklearn.metrics
import sklearn.pipeline

import const
import tasks_ml_prep


class Keyer:
    """Object producing keys for a set of models or instances.

    Object which generates keys describing for which types of predictions a set of models should be
    used for. This may also be used to partition data or requests by the prediction task for which
    they are relevant.
    """

    def __init__(self, key_cols):
        """Create a new keyer.

        Args:
            key_cols: List of attribute names from which the key should be constructed.
        """
        self._key_cols = key_cols

    def get(self, record):
        """Get a key for an instance.

        Args:
            Record describing either the task or data point being considered.

        Returns:
            Key which can be used to identify the data or model relevant to the modeling task.
        """
        return ','.join(map(lambda x: record[x], self._key_cols))


class RegressorInputGetter:
    """Object to get simple flat input vectors for a curve model.

    Object to get simple flat input vectors for a curve model, useful for ensuring standardized
    input for training or model execution.
    """

    def __init__(self, input_cols):
        """Create a new input getter which standardizes inputs to a model.

        Args:
            input_cols: The list of column or attributes names in the order they should appear as
                input vectors to the model.
        """
        self._input_cols = input_cols

    def get(self, record):
        """Build an input vector for the input record.

        Args:
            record: The record from which a model-compatible flat input vector should be built. This
                may be an input training instance, for example.

        Returns:
            List / simple vector that can be fed into the model.
        """
        return [record[x] for x in self._input_cols]


class KeyedModel:
    """Model adapter which uses different children models based on task.

    Model collection which determines which model should be used based on input vectors, offering a
    scikit-learn predict-like method for polymorphism.
    """

    def __init__(self, models, keyer, input_getter):
        """Create a new keyed model collection.

        Args:
            models: Models in a dictionary structure where the key is the key for tasks for which
                they apply and the value is the scikit learn or equivalent-interfaced model.
            keyer: Keyer which can be used to generate keys in the form exected by models when given
                input values or tasks.
            input_getter: Object to convert input instances or tasks to a flat input vector usable
                by any model in models.
        """
        self._models = models
        self._keyer = keyer
        self._input_getter = input_getter

    def predict(self, target):
        """Predict the plastics outcome for the given input tasks.

        Predict the plastics outcome for the given input tasks, switching between target model based
        on the input task and the keyer behind this KeyedModel.

        Args:
            target: List of dictionaries describing the tasks for which a prediction should be
                returned.

        Returns:
            Predictions corresponding to the input tasks in target in the same order as target.
        """
        ret_list = []

        for row in target:
            key = self._keyer.get(row)
            model = self._models[key]
            inputs = self._input_getter.get(row)
            prediction = model.predict([inputs])[0]
            ret_list.append(prediction)

        return ret_list


class CurveTask(luigi.Task):
    """Template Method to fit curves for predicting plastics futures.

    Template Method for a Luigi Task which can be used to fit curves either in the "naive" or
    "curve" configuration to predict plastic future outcomes like consumption, waste, and trade.
    Individual models needing training should subclass this template one subclass per model
    required.
    """

    def run(self):
        """Perform a sweep to construct a model.

        Train the model with a small sweep of hyperparameters and evaluate performance metrics
        including validation set performance.
        """
        with self.input().open('r') as f:
            job_info = json.load(f)

        instances = self.load_instances(job_info)

        self.assign_sets(instances)

        training_results = self.sweep(instances)
        training_results_standard = self.standardize_results(training_results)

        force_type = job_info.get('force_type', None)

        if force_type:
            training_results_standard_allowed = filter(
                lambda x: x['type'] == force_type,
                training_results_standard
            )
        else:
            training_results_standard_allowed = training_results_standard

        selected_option = min(
            training_results_standard_allowed,
            key=lambda x: x['valid']
        )

        final_model = self.train(
            selected_option,
            instances,
            [target[self.get_response_col()] for target in instances],
        )

        sweep_results_loc = os.path.join(
            job_info['directories']['output'],
            self.get_report_filename()
        )
        with open(sweep_results_loc, 'w') as f:
            fieldnames = training_results_standard[0].keys()
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(training_results_standard)

        model_loc = os.path.join(
            job_info['directories']['workspace'],
            self.get_model_filename()
        )
        with open(model_loc, 'wb') as f:
            pickle.dump(final_model, f)

        with self.output().open('w') as f:
            return json.dump(job_info, f)

    def load_instances(self, job_info):
        """Load instances that will be split into train, test, validation.

        Args:
            job_info: Information about the job which includes details required to determine from
                where the instances should be loaded. Specifically, information required to
                find the scratch SQLite database.

        Returns:
            Instances as a list of dictionaries with one dictionary per instance.
        """
        database_loc = job_info['database']
        connection = sqlite3.connect(database_loc)

        cursor = connection.cursor()

        cursor.execute(self.get_sql())

        def parse_row(raw_row):
            return dict(zip(self.get_cols(), raw_row))

        ret_data = [parse_row(row) for row in cursor.fetchall()]

        cursor.close()
        connection.close()

        return ret_data

    def assign_sets(self, instances):
        """Split instances across different sets (validation, train, ignore).

        Split instances across different sets (validation, train, ignore), recognizing that some
        instances may be from anamalous years like COVID / 2020 and should be excluded as to prevent
        skewing training.

        Args:
            instances: The instances to split across different sets. These instances will be udpated
                in place with a new setAssign attribute. If setAssign was present perviously, it
                will be overwritten.
        """
        for instance in instances:
            out_sample = self.is_out_sample_candidate(instance)
            ignore = self.is_out_sample_ignore(instance)

            if out_sample:
                out_sample_label = 'ignore' if ignore else 'valid'
            else:
                out_sample_label = 'train'

            instance['setAssign'] = out_sample_label

    def sweep(self, instances):
        """Perform the sweep across hyperparameters.

        Args:
            instances: Instances with set assignment (setAssign) on which training and evaluation
                should be performed.

        Returns:
            Results of training as a list of dictionaries with one dict per model trainined where
            each record has information about perforamnce as well as a reference to the built model
            under the model attribute.
        """

        def get_set_instances(label):
            return filter(lambda x: x['setAssign'] == label, instances)

        def get_set_inputs(label):

            def get_inputs(target):
                return dict(map(
                    lambda col: (col, target[col]),
                    self.get_input_cols() + self.get_key_cols()
                ))

            set_instances = get_set_instances(label)
            inputs = map(get_inputs, set_instances)
            return list(inputs)

        def get_set_response(label):
            set_instances = get_set_instances(label)
            inputs = map(lambda x: x[self.get_response_col()], set_instances)
            return list(inputs)

        train_inputs = get_set_inputs('train')
        train_response = get_set_response('train')

        valid_inputs = get_set_inputs('valid')
        valid_response = get_set_response('valid')

        def evaluate_response(model):

            def evaluate_single(target_inputs, actual):
                predicted = model.predict(target_inputs)
                return sklearn.metrics.mean_absolute_error(predicted, actual)

            return {
                'train': evaluate_single(train_inputs, train_response),
                'valid': evaluate_single(valid_inputs, valid_response)
            }

        queue = []

        for degree in [1, 2]:
            queue.append({
                'type': 'linear',
                'degree': degree
            })

        def execute_task(task):
            output_record = {}
            output_record.update(task)

            model = self.train(task, train_inputs, train_response)
            output_record.update(evaluate_response(model['model']))

            return output_record

        return [execute_task(task) for task in queue]

    def standardize_results(self, results):
        """Ensure all records for built models have the same attributes.

        Different models will report different information about their hyperparameters. This will
        write empty values into the model records such that all records have the same set of
        attributes even if some are empty because they are not relevant for the model trained.

        Args:
            results: List of model records (as dictionaries) to standardize.

        Returns:
            List of dicts after standardization.
        """
        keys_per_row = map(lambda x: x.keys(), results)
        keys_iter = itertools.chain(*keys_per_row)
        keys_allowed = filter(lambda x: x != 'model', keys_iter)
        keys_set = sorted(set(keys_allowed))

        def standardize_result(result):
            values = map(lambda x: result.get(x, ''), keys_set)
            return dict(zip(keys_set, values))

        return [standardize_result(x) for x in results]

    def train(self, option, train_inputs, train_response):
        if option['type'] == 'linear':
            return self.try_linear(
                option['degree'],
                train_inputs,
                train_response
            )
        else:
            raise RuntimeError('Unrecognized option type ' + option['type'])

    def try_linear(self, degree, train_inputs, train_response):
        """Build a linear model.

        Build a linear model which may use polynomial features inside through scikit learn pipeline.

        Args:
            degree: The degree of the function to fit. A degree of 1 is simple line fitting.
            train_inputs: Collection of instances for training.
            train_response: Collection of response values paired to train_inputs.

        Returns:
            Dictionary describing the model including the degree.
        """
        key_cols = sorted(self.get_key_cols())
        input_cols = sorted(self.get_input_cols())

        keyer = Keyer(key_cols)
        input_getter = RegressorInputGetter(input_cols)

        inputs_by_key = {}
        response_by_key = {}
        for (train_input, train_response) in zip(train_inputs, train_response):
            key = keyer.get(train_input)

            if key not in inputs_by_key:
                inputs_by_key[key] = []
                response_by_key[key] = []

            regressor_inputs = input_getter.get(train_input)

            inputs_by_key[key].append(regressor_inputs)
            response_by_key[key].append(train_response)

        keys = inputs_by_key.keys()
        models_inner = {}
        for key in keys:
            if degree == 1:
                model = sklearn.linear_model.LinearRegression()
            else:
                model = sklearn.pipeline.Pipeline([
                    ('poly', sklearn.preprocessing.PolynomialFeatures(degree=degree)),
                    ('linear', sklearn.linear_model.LinearRegression(fit_intercept=False))
                ])

            model.fit(inputs_by_key[key], response_by_key[key])
            models_inner[key] = model

        return {
            'degree': degree,
            'model': KeyedModel(models_inner, keyer, input_getter)
        }

    def is_out_sample_candidate(self, target):
        """Determine if an instance should be labeled as out of sample.

        Determine if an instance is out of sample and should be set aside for the out of sample
        test.

        Args:
            target: The instance to be labeled.

        Returns:
            True if out of sample and False if in sample.
        """
        return target['year'] >= 2019

    def is_out_sample_ignore(self, target):
        """Determine if an instance should be ignored.

        Args:
            target: The instance to be labeled.

        Returns:
            True if from an anamalous year (like COVID, 2020). False otherwise.
        """
        return target['year'] == 2020

    def get_model_filename(self):
        """Determine the filename at which the fit model should be pickeled.

        Returns:
            String path to where the model should be written.
        """
        raise NotImplementedError('Use implementor.')

    def get_key_cols(self):
        """Determine which columsn define which model should be used.

        Returns:
            Columns to be used in a Keyer.
        """
        raise NotImplementedError('Use implementor.')

    def get_sql(self):
        """Get the SQL which is used to query for instances.

        Returns:
            SQL to retrieve instance which may be used for training, validation, or test.
        """
        raise NotImplementedError('Use implementor.')

    def get_cols(self):
        """Get the columns that will be returned from querying with get_sql.

        Returns:
            The in-order list of columns returned by the query described at get_sql.
        """
        raise NotImplementedError('Use implementor.')

    def get_input_cols(self):
        """Get the columns that that are inputs to the model.

        Returns:
            The list of columns in-order that are inputs into the model.
        """
        raise NotImplementedError('Use implementor.')

    def get_response_col(self):
        """Get the column describing the response variable to be predicted.

        Returns:
            The name of the column in which the response variable can be found.
        """
        raise NotImplementedError('Use implementor.')

    def get_report_filename(self):
        """Get the filename at which the sweep report should be written.

        Returns:
            The path to where the sweep report should be written.
        """
        raise NotImplementedError('Use implementor.')


class ConsumptionCurveTask(CurveTask):
    """Train a "curve" model that predicts consumption."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        """Require that the instance data are ready for training."""
        return tasks_ml_prep.CheckMlConsumptionViewTask(task_dir=self.task_dir)

    def output(self):
        """Report that a curve model has been trained."""
        out_path = os.path.join(self.task_dir, '400_consumption_curve.json')
        return luigi.LocalTarget(out_path)

    def get_sql(self):
        """Get the SQL query which returns instances for this sweep.

        Returns:
            The SQL query for this sweep.
        """
        return '''
            SELECT
                year,
                region,
                population,
                gdp,
                majorMarketSector,
                consumptionMT
            FROM
                instance_consumption_normal
        '''

    def get_key_cols(self):
        """Indicate that consumption has one model per region / sector.

        Returns:
            List of keys for Keyer.
        """
        return [
            'region',
            'majorMarketSector'
        ]

    def get_cols(self):
        """Get the columns that are returned by executing the query from get_sql.

        Returns:
            List of columns that would be returned by running the query at get_sql.
        """
        return [
            'year',
            'region',
            'population',
            'gdp',
            'majorMarketSector',
            'consumptionMT'
        ]

    def get_input_cols(self):
        """Indicate that population and GDP are inputs to the curve model.

        Returns:
            List of columns to use as inputs.
        """
        return [
            'population',
            'gdp'
        ]

    def get_response_col(self):
        """Indicate that consumption is the response variable.

        Returns:
            The name of the consumption column.
        """
        return 'consumptionMT'

    def get_report_filename(self):
        """Indicate where the curve fitting report should be written.

        Returns:
            The name of the file where sweep results should be written.
        """
        return 'consumption_curve.csv'

    def get_model_filename(self):
        """Indicate where the curve model should be pickeled.

        Returns:
            The name of the pickle file to write.
        """
        return 'consumption_curve.pickle'


class ConsumptionCurveNaiveTask(CurveTask):
    """Train a "naive" model that predicts consumption."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        """Require that the instance data are ready for training."""
        return tasks_ml_prep.CheckMlConsumptionViewTask(task_dir=self.task_dir)

    def output(self):
        """Report that a naive model has been trained."""
        out_path = os.path.join(self.task_dir, '401_consumption_curve_naive.json')
        return luigi.LocalTarget(out_path)

    def get_sql(self):
        """Get the SQL query which returns instances for this sweep.

        Returns:
            The SQL query for this sweep.
        """
        return '''
            SELECT
                year,
                region,
                population,
                gdp,
                majorMarketSector,
                consumptionMT
            FROM
                instance_consumption_normal
        '''

    def get_key_cols(self):
        """Indicate that consumption has one model per region / sector.

        Returns:
            List of keys for Keyer.
        """
        return [
            'region',
            'majorMarketSector'
        ]

    def get_cols(self):
        """Get the columns that are returned by executing the query from get_sql.

        Returns:
            List of columns that would be returned by running the query at get_sql.
        """
        return [
            'year',
            'region',
            'population',
            'gdp',
            'majorMarketSector',
            'consumptionMT'
        ]

    def get_input_cols(self):
        """Indicate that year is the only input to the naive model.

        Returns:
            List of columns to use as inputs.
        """
        return [
            'year'
        ]

    def get_response_col(self):
        """Indicate that consumption is the response variable.

        Returns:
            The name of the consumption column.
        """
        return 'consumptionMT'

    def get_report_filename(self):
        """Indicate where the naive fitting report should be written.

        Returns:
            The name of the file where sweep results should be written.
        """
        return 'consumption_curve_naive.csv'

    def get_model_filename(self):
        """Indicate where the naive model should be pickeled.

        Returns:
            The name of the pickle file to write.
        """
        return 'consumption_curve_naive.pickle'


class WasteCurveTask(CurveTask):
    """Task which fits a curve to predict waste EOL fate."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        """Require that the instance data are ready for training."""
        return tasks_ml_prep.CheckMlWasteViewTask(task_dir=self.task_dir)

    def output(self):
        """Report that a curve model has been trained."""
        out_path = os.path.join(self.task_dir, '402_waste_curve.json')
        return luigi.LocalTarget(out_path)

    def get_sql(self):
        """Get the SQL query which returns instances for this sweep.

        Returns:
            The SQL query for this sweep.
        """
        return '''
            SELECT
                year,
                region,
                type,
                percent,
                population,
                gdp
            FROM
                instance_waste_normal
        '''

    def get_key_cols(self):
        """Indicate that waste has one model per region / fate.

        Returns:
            List of keys for Keyer.
        """
        return [
            'region',
            'type'
        ]

    def get_cols(self):
        """Get the columns that are returned by executing the query from get_sql.

        Returns:
            List of columns that would be returned by running the query at get_sql.
        """
        return [
            'year',
            'region',
            'type',
            'percent',
            'population',
            'gdp'
        ]

    def get_input_cols(self):
        """Indicate that population and GDP are inputs to the curve model.

        Returns:
            List of columns to use as inputs.
        """
        return [
            'population',
            'gdp'
        ]

    def get_response_col(self):
        """Indicate that EOL propensity is the response variable.

        Returns:
            The name of the EOL column.
        """
        return 'percent'

    def get_report_filename(self):
        """Indicate where the curve fitting report should be written.

        Returns:
            The name of the file where sweep results should be written.
        """
        return 'waste_curve.csv'

    def get_model_filename(self):
        """Indicate where the curve model should be pickeled.

        Returns:
            The name of the pickle file to write.
        """
        return 'waste_curve.pickle'


class WasteCurveNaiveTask(CurveTask):
    """Train a "naive" model that predicts EOL fate propensity."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        """Require that the instance data are ready for training."""
        return tasks_ml_prep.CheckMlWasteViewTask(task_dir=self.task_dir)

    def output(self):
        """Report that a naive model has been trained."""
        out_path = os.path.join(self.task_dir, '403_waste_curve_naive.json')
        return luigi.LocalTarget(out_path)

    def get_sql(self):
        """Get the SQL query which returns instances for this sweep.

        Returns:
            The SQL query for this sweep.
        """
        return '''
            SELECT
                year,
                region,
                type,
                percent,
                population,
                gdp
            FROM
                instance_waste_normal
        '''

    def get_key_cols(self):
        """Indicate that consumption has one model per region / fate.

        Returns:
            List of keys for Keyer.
        """
        return [
            'region',
            'type'
        ]

    def get_cols(self):
        """Get the columns that are returned by executing the query from get_sql.

        Returns:
            List of columns that would be returned by running the query at get_sql.
        """
        return [
            'year',
            'region',
            'type',
            'percent',
            'population',
            'gdp'
        ]

    def get_input_cols(self):
        """Indicate that year is the only input to the naive model.

        Returns:
            List of columns to use as inputs.
        """
        return [
            'year'
        ]

    def get_response_col(self):
        """Indicate that EOL fate propensity is the response variable.

        Returns:
            The name of the EOL fate column.
        """
        return 'percent'

    def get_report_filename(self):
        """Indicate where the naive fitting report should be written.

        Returns:
            The name of the file where sweep results should be written.
        """
        return 'waste_curve_naive.csv'

    def get_model_filename(self):
        """Indicate where the naive model should be pickeled.

        Returns:
            The name of the pickle file to write.
        """
        return 'waste_curve_naive.pickle'


class TradeCurveTask(CurveTask):
    """Train a "curve" model that predicts trade (goods and materials)."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        """Require that the instance data are ready for training."""
        return tasks_ml_prep.CheckMlTradeViewTask(task_dir=self.task_dir)

    def output(self):
        """Report that a curve model has been trained."""
        out_path = os.path.join(self.task_dir, '404_trade_curve.json')
        return luigi.LocalTarget(out_path)

    def get_sql(self):
        """Get the SQL query which returns instances for this sweep.

        Returns:
            The SQL query for this sweep.
        """
        return '''
            SELECT
                year,
                region,
                type,
                netMT,
                population,
                gdp
            FROM
                instance_trade_normal
        '''

    def get_key_cols(self):
        """Indicate that consumption has one model per region / type.

        Returns:
            List of keys for Keyer.
        """
        return [
            'region',
            'type'
        ]

    def get_cols(self):
        """Get the columns that are returned by executing the query from get_sql.

        Returns:
            List of columns that would be returned by running the query at get_sql.
        """
        return [
            'year',
            'region',
            'type',
            'netMT',
            'population',
            'gdp'
        ]

    def get_input_cols(self):
        """Indicate that population and GDP are inputs to the curve model.

        Returns:
            List of columns to use as inputs.
        """
        return [
            'population',
            'gdp'
        ]

    def get_response_col(self):
        """Indicate that trade (goods / materials) is the response variable.

        Returns:
            The name of the trade column.
        """
        return 'netMT'

    def get_report_filename(self):
        """Indicate where the curve fitting report should be written.

        Returns:
            The name of the file where sweep results should be written.
        """
        return 'trade_curve.csv'

    def get_model_filename(self):
        """Indicate where the curve model should be pickeled.

        Returns:
            The name of the pickle file to write.
        """
        return 'trade_curve.pickle'


class TradeCurveNaiveTask(CurveTask):
    """Train a "naive" model that predicts trade (goods and materials)."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        """Require that the instance data are ready for training."""
        return tasks_ml_prep.CheckMlTradeViewTask(task_dir=self.task_dir)

    def output(self):
        """Report that a naive model has been trained."""
        out_path = os.path.join(self.task_dir, '405_trade_curve_naive.json')
        return luigi.LocalTarget(out_path)

    def get_sql(self):
        """Get the SQL query which returns instances for this sweep.

        Returns:
            The SQL query for this sweep.
        """
        return '''
            SELECT
                year,
                region,
                type,
                netMT,
                population,
                gdp
            FROM
                instance_trade_normal
        '''

    def get_key_cols(self):
        """Indicate that consumption has one model per region / fate.

        Returns:
            List of keys for Keyer.
        """
        return [
            'region',
            'type'
        ]

    def get_cols(self):
        """Get the columns that are returned by executing the query from get_sql.

        Returns:
            List of columns that would be returned by running the query at get_sql.
        """
        return [
            'year',
            'region',
            'type',
            'netMT',
            'population',
            'gdp'
        ]

    def get_input_cols(self):
        """Indicate that year is the only input to the naive model.

        Returns:
            List of columns to use as inputs.
        """
        return [
            'year'
        ]

    def get_response_col(self):
        """Indicate that consumption is the response variable.

        Returns:
            The name of the consumption column.
        """
        return 'netMT'

    def get_report_filename(self):
        """Indicate where the naive fitting report should be written.

        Returns:
            The name of the file where sweep results should be written.
        """
        return 'trade_curve_naive.csv'

    def get_model_filename(self):
        """Indicate where the naive model should be pickeled.

        Returns:
            The name of the pickle file to write.
        """
        return 'trade_curve_naive.pickle'


class WasteTradeCurveTask(CurveTask):
    """Train a "curve" model that predicts waste trade."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        """Require that the instance data are ready for training."""
        return tasks_ml_prep.CheckMlWasteTradeViewTask(task_dir=self.task_dir)

    def output(self):
        """Report that a curve model has been trained."""
        out_path = os.path.join(self.task_dir, '406_waste_trade_curve.json')
        return luigi.LocalTarget(out_path)

    def get_sql(self):
        """Get the SQL query which returns instances for this sweep.

        Returns:
            The SQL query for this sweep.
        """
        return '''
            SELECT
                year,
                region,
                netMT,
                population,
                gdp
            FROM
                instance_waste_trade_normal
            WHERE
                year > 2007
        '''

    def get_key_cols(self):
        """Indicate that consumption has one model per region.

        Returns:
            List of keys for Keyer.
        """
        return [
            'region'
        ]

    def get_cols(self):
        """Get the columns that are returned by executing the query from get_sql.

        Returns:
            List of columns that would be returned by running the query at get_sql.
        """
        return [
            'year',
            'region',
            'netMT',
            'population',
            'gdp'
        ]

    def get_input_cols(self):
        """Indicate that population and GDP are inputs to the curve model.

        Returns:
            List of columns to use as inputs.
        """
        return [
            'population',
            'gdp'
        ]

    def get_response_col(self):
        """Indicate that trade (waste) is the response variable.

        Returns:
            The name of the trade column.
        """
        return 'netMT'

    def get_report_filename(self):
        """Indicate where the curve fitting report should be written.

        Returns:
            The name of the file where sweep results should be written.
        """
        return 'wasteTrade_curve.csv'

    def get_model_filename(self):
        """Indicate where the curve model should be pickeled.

        Returns:
            The name of the pickle file to write.
        """
        return 'wasteTrade_curve.pickle'


class WasteTradeCurveNaiveTask(CurveTask):
    """Train a "naive" model that predicts trade (waste)."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        """Require that the instance data are ready for training."""
        return tasks_ml_prep.CheckMlWasteTradeViewTask(task_dir=self.task_dir)

    def output(self):
        """Report that a naive model has been trained."""
        out_path = os.path.join(self.task_dir, '407_trade_curve_naive.json')
        return luigi.LocalTarget(out_path)

    def get_sql(self):
        """Get the SQL query which returns instances for this sweep.

        Returns:
            The SQL query for this sweep.
        """
        return '''
            SELECT
                year,
                region,
                netMT,
                population,
                gdp
            FROM
                instance_waste_trade_normal
            WHERE
                year > 2007
        '''

    def get_key_cols(self):
        """Indicate that consumption has one model per region.

        Returns:
            List of keys for Keyer.
        """
        return [
            'region'
        ]

    def get_cols(self):
        """Get the columns that are returned by executing the query from get_sql.

        Returns:
            List of columns that would be returned by running the query at get_sql.
        """
        return [
            'year',
            'region',
            'netMT',
            'population',
            'gdp'
        ]

    def get_input_cols(self):
        """Indicate that year is the only input to the naive model.

        Returns:
            List of columns to use as inputs.
        """
        return [
            'year'
        ]

    def get_response_col(self):
        """Indicate that waste trade is the response variable.

        Returns:
            The name of the trade column.
        """
        return 'netMT'

    def get_report_filename(self):
        """Indicate where the naive fitting report should be written.

        Returns:
            The name of the file where sweep results should be written.
        """
        return 'wasteTrade_curve_naive.csv'

    def get_model_filename(self):
        """Indicate where the naive model should be pickeled.

        Returns:
            The name of the pickle file to write.
        """
        return 'wasteTrade_curve_naive.pickle'
