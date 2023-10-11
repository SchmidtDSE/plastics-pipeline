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

    def __init__(self, key_cols):
        self._key_cols = key_cols

    def get(self, record):
        return ','.join(map(lambda x: record[x], self._key_cols))


class RegressorInputGetter:

    def __init__(self, input_cols):
        self._input_cols = input_cols

    def get(self, record):
        return [record[x] for x in self._input_cols]


class KeyedModel:

    def __init__(self, models, keyer, input_getter):
        self._models = models
        self._keyer = keyer
        self._input_getter = input_getter

    def predict(self, target):
        ret_list = []

        for row in target:
            key = self._keyer.get(row)
            model = self._models[key]
            inputs = self._input_getter.get(row)
            prediction = model.predict([inputs])[0]
            ret_list.append(prediction)

        return ret_list


class CurveTask(luigi.Task):

    def run(self):
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
        for instance in instances:
            out_sample = self.is_out_sample_candidate(instance)
            ignore = self.is_out_sample_ignore(instance)

            if out_sample:
                out_sample_label = 'ignore' if ignore else 'valid'
            else:
                out_sample_label = 'train'

            instance['setAssign'] = out_sample_label

    def sweep(self, instances):

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
        return target['year'] >= 2019

    def is_out_sample_ignore(self, target):
        return target['year'] == 2020

    def get_model_filename(self):
        raise NotImplementedError('Use implementor.')

    def get_key_cols(self):
        raise NotImplementedError('Use implementor.')

    def get_sql(self):
        raise NotImplementedError('Use implementor.')

    def get_cols(self):
        raise NotImplementedError('Use implementor.')
    
    def get_input_cols(self):
        raise NotImplementedError('Use implementor.')

    def get_response_col(self):
        raise NotImplementedError('Use implementor.')

    def get_report_filename(self):
        raise NotImplementedError('Use implementor.')


class ConsumptionCurveTask(CurveTask):

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return tasks_ml_prep.CheckMlConsumptionViewTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '400_consumption_curve.json')
        return luigi.LocalTarget(out_path)

    def get_sql(self):
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
        return [
            'region',
            'majorMarketSector'
        ]

    def get_cols(self):
        return [
            'year',
            'region',
            'population',
            'gdp',
            'majorMarketSector',
            'consumptionMT'
        ]
    
    def get_input_cols(self):
        return [
            'population',
            'gdp'
        ]

    def get_response_col(self):
        return 'consumptionMT'

    def get_report_filename(self):
        return 'consumption_curve.csv'

    def get_model_filename(self):
        return 'consumption_curve.pickle'


class ConsumptionCurveNaiveTask(CurveTask):

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return tasks_ml_prep.CheckMlConsumptionViewTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '401_consumption_curve_naive.json')
        return luigi.LocalTarget(out_path)

    def get_sql(self):
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
        return [
            'region',
            'majorMarketSector'
        ]

    def get_cols(self):
        return [
            'year',
            'region',
            'population',
            'gdp',
            'majorMarketSector',
            'consumptionMT'
        ]
    
    def get_input_cols(self):
        return [
            'year'
        ]

    def get_response_col(self):
        return 'consumptionMT'

    def get_report_filename(self):
        return 'consumption_curve_naive.csv'

    def get_model_filename(self):
        return 'consumption_curve_naive.pickle'


class WasteCurveTask(CurveTask):

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return tasks_ml_prep.CheckMlWasteViewTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '402_waste_curve.json')
        return luigi.LocalTarget(out_path)

    def get_sql(self):
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
        return [
            'region',
            'type'
        ]

    def get_cols(self):
        return [
            'year',
            'region',
            'type',
            'percent',
            'population',
            'gdp'
        ]
    
    def get_input_cols(self):
        return [
            'population',
            'gdp'
        ]

    def get_response_col(self):
        return 'percent'

    def get_report_filename(self):
        return 'waste_curve.csv'

    def get_model_filename(self):
        return 'waste_curve.pickle'


class WasteCurveNaiveTask(CurveTask):

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return tasks_ml_prep.CheckMlWasteViewTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '403_waste_curve_naive.json')
        return luigi.LocalTarget(out_path)

    def get_sql(self):
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
        return [
            'region',
            'type'
        ]

    def get_cols(self):
        return [
            'year',
            'region',
            'type',
            'percent',
            'population',
            'gdp'
        ]
    
    def get_input_cols(self):
        return [
            'year'
        ]

    def get_response_col(self):
        return 'percent'

    def get_report_filename(self):
        return 'waste_curve_naive.csv'

    def get_model_filename(self):
        return 'waste_curve_naive.pickle'


class TradeCurveTask(CurveTask):

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return tasks_ml_prep.CheckMlTradeViewTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '404_trade_curve.json')
        return luigi.LocalTarget(out_path)

    def get_sql(self):
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
        return [
            'region',
            'type'
        ]

    def get_cols(self):
        return [
            'year',
            'region',
            'type',
            'netMT',
            'population',
            'gdp'
        ]
    
    def get_input_cols(self):
        return [
            'population',
            'gdp'
        ]

    def get_response_col(self):
        return 'netMT'

    def get_report_filename(self):
        return 'trade_curve.csv'

    def get_model_filename(self):
        return 'trade_curve.pickle'


class TradeCurveNaiveTask(CurveTask):

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return tasks_ml_prep.CheckMlTradeViewTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '405_trade_curve_naive.json')
        return luigi.LocalTarget(out_path)

    def get_sql(self):
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
        return [
            'region',
            'type'
        ]

    def get_cols(self):
        return [
            'year',
            'region',
            'type',
            'netMT',
            'population',
            'gdp'
        ]
    
    def get_input_cols(self):
        return [
            'year'
        ]

    def get_response_col(self):
        return 'netMT'

    def get_report_filename(self):
        return 'trade_curve_naive.csv'

    def get_model_filename(self):
        return 'trade_curve_naive.pickle'


class WasteTradeCurveTask(CurveTask):

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return tasks_ml_prep.CheckMlWasteTradeViewTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '406_waste_trade_curve.json')
        return luigi.LocalTarget(out_path)

    def get_sql(self):
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
        return [
            'region'
        ]

    def get_cols(self):
        return [
            'year',
            'region',
            'netMT',
            'population',
            'gdp'
        ]
    
    def get_input_cols(self):
        return [
            'population',
            'gdp'
        ]

    def get_response_col(self):
        return 'netMT'

    def get_report_filename(self):
        return 'wasteTrade_curve.csv'

    def get_model_filename(self):
        return 'wasteTrade_curve.pickle'


class WasteTradeCurveNaiveTask(CurveTask):

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return tasks_ml_prep.CheckMlWasteTradeViewTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '407_trade_curve_naive.json')
        return luigi.LocalTarget(out_path)

    def get_sql(self):
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
        return [
            'region'
        ]

    def get_cols(self):
        return [
            'year',
            'region',
            'netMT',
            'population',
            'gdp'
        ]
    
    def get_input_cols(self):
        return [
            'year'
        ]

    def get_response_col(self):
        return 'netMT'

    def get_report_filename(self):
        return 'wasteTrade_curve_naive.csv'

    def get_model_filename(self):
        return 'wasteTrade_curve_naive.pickle'
