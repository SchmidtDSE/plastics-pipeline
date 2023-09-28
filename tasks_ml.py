import csv
import itertools
import json
import os
import pickle
import random
import sqlite3
import statistics

import luigi
import pathos.pools
import sklearn.ensemble
import sklearn.linear_model
import sklearn.metrics
import sklearn.pipeline
import sklearn.preprocessing
import sklearn.svm
import sklearn.tree

import const
import tasks_ml_prep


class DecoratedModel:

    def __init__(self, inner_model, cols):
        self._inner_model = inner_model
        self._cols = cols

    def predict(self, targets):
        targets_linear = [[target[col] for col in self._cols] for target in targets]
        return self._inner_model.predict(targets_linear)


class SweepTask(luigi.Task):

    def run(self):
        with self.input().open('r') as f:
            job_info = json.load(f)

        instances = self.load_instances(job_info)

        self.assign_sets(instances)

        training_results = self.sweep(instances, job_info['workers'])
        training_results_standard = self.standardize_results(training_results)

        force_type = job_info["forceModels"].get(self.get_model_class(), None)

        if force_type:
            training_results_standard_allowed = filter(
                lambda x: x['type'] == force_type,
                training_results_standard
            )
        else:
            training_results_standard_allowed = training_results_standard

        selected_option = min(
            training_results_standard_allowed,
            key=lambda x: x['validInSampleResponse']
        )

        final_model = self.train(
            selected_option,
            [[target[col] for col in self.get_input_cols()] for target in instances],
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
            final_model['model'] = DecoratedModel(
                final_model['model'],
                self.get_input_cols()
            )
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

            instance['setAssignOutSample'] = out_sample_label
            instance['setAssignInSample'] = instance['setAssign'] = random.choice(
                ['test', 'valid', 'train', 'train', 'train', 'train']
            )

    def sweep(self, instances, workers):

        def get_set_instances(label, set_type):
            return filter(lambda x: x[set_type] == label, instances)

        def get_set_inputs(label, set_type):

            def get_inputs(target):
                return [target[col] for col in self.get_input_cols()]

            set_instances = get_set_instances(label, set_type)
            inputs = map(get_inputs, set_instances)
            return list(inputs)

        def get_set_response(label, set_type):
            set_instances = get_set_instances(label, set_type)
            inputs = map(lambda x: x[self.get_response_col()], set_instances)
            return list(inputs)

        def get_set_target(label, set_type):

            def get_outputs(target):
                return [target[col] for col in self.get_output_cols()]

            set_instances = get_set_instances(label, set_type)
            inputs = map(get_outputs, set_instances)
            return list(inputs)

        samples = {
            'inSample': {
                'train': {
                    'inputs': get_set_inputs('train', 'setAssignInSample'),
                    'response': get_set_response('train', 'setAssignInSample'),
                    'target': get_set_target('train', 'setAssignInSample')
                },
                'valid': {
                    'inputs': get_set_inputs('valid', 'setAssignInSample'),
                    'response': get_set_response('valid', 'setAssignInSample'),
                    'target': get_set_target('valid', 'setAssignInSample')
                },
                'test': {
                    'inputs': get_set_inputs('test', 'setAssignInSample'),
                    'response': get_set_response('test', 'setAssignInSample'),
                    'target': get_set_target('test', 'setAssignInSample')
                }
            },
            'outSample': {
                'train': {
                    'inputs': get_set_inputs('train', 'setAssignOutSample'),
                    'response': get_set_response('train', 'setAssignOutSample'),
                    'target': get_set_target('train', 'setAssignOutSample')
                },
                'valid': {
                    'inputs': get_set_inputs('valid', 'setAssignOutSample'),
                    'response': get_set_response('valid', 'setAssignOutSample'),
                    'target': get_set_target('valid', 'setAssignOutSample')
                },
                'test': {
                    'inputs': get_set_inputs('valid', 'setAssignOutSample'),
                    'response': get_set_response('valid', 'setAssignOutSample'),
                    'target': get_set_target('valid', 'setAssignOutSample')
                }
            }
        }

        def evaluate_response(model, label, target_set):

            def evaluate_single(target_inputs, actual):
                predicted = model.predict(target_inputs)
                return sklearn.metrics.mean_absolute_error(predicted, actual)
            
            return {
                'train' + label: evaluate_single(
                    target_set['train']['inputs'],
                    target_set['train']['response']
                ),
                'valid' + label: evaluate_single(
                    target_set['valid']['inputs'],
                    target_set['valid']['response']
                ),
                'test' + label: evaluate_single(
                    target_set['test']['inputs'],
                    target_set['test']['response']
                )
            }

        def evaluate_target(model, label, target_set):

            return {
                'train' + label: self.evaluate_target_set(
                    model.predict(target_set['train']['inputs']),
                    target_set['train']['target']
                ),
                'valid' + label: self.evaluate_target_set(
                    model.predict(target_set['valid']['inputs']),
                    target_set['valid']['target']
                ),
                'test' + label: self.evaluate_target_set(
                    model.predict(target_set['test']['inputs']),
                    target_set['test']['target']
                )
            }

        queue = []

        for alpha in [0, 0.2, 0.4, 0.6, 0.8, 1]:
            queue.append({
                'type': 'linear',
                'alpha': alpha
            })

        if self.get_svm_enabled():
            for kernel in ['linear', 'poly', 'rbf']:
                for degree in [1, 2, 3, 4]:
                    for alpha in [0, 0.2, 0.4, 0.6, 0.8]:
                        queue.append({
                            'type': 'svr',
                            'kernel': kernel,
                            'degree': degree,
                            'alpha': alpha
                        })

        for depth in range(2, 30):
            queue.append({
                'type': 'tree',
                'depth': depth
            })

        for depth in range(2, 20):
            for estimators in [5, 10, 15, 20, 25, 30]:
                queue.append({
                    'type': 'adaboost',
                    'depth': depth,
                    'estimators': estimators
                })

        for depth in range(2, 20):
            for estimators in [5, 10, 15, 20, 25, 30]:
                for max_features in [1, 'sqrt', 'log2']:
                    queue.append({
                        'type': 'random forest',
                        'depth': depth,
                        'estimators': estimators,
                        'max_features': max_features
                    })

        def execute_task(task):
            output_record = {}
            output_record.update(task)

            model_in_sample = self.train(
                task,
                samples['inSample']['train']['inputs'],
                samples['inSample']['train']['response']
            )
            output_record.update(evaluate_response(
                model_in_sample['model'],
                'InSampleResponse',
                samples['inSample']
            ))
            output_record.update(evaluate_target(
                model_in_sample['model'],
                'InSampleTarget',
                samples['inSample']
            ))

            model_out_sample = self.train(
                task,
                samples['outSample']['train']['inputs'],
                samples['outSample']['train']['response']
            )
            output_record.update(evaluate_response(
                model_out_sample['model'],
                'OutSampleResponse',
                samples['outSample']
            ))
            output_record.update(evaluate_target(
                model_out_sample['model'],
                'OutSampleTarget',
                samples['outSample']
            ))

            return output_record

        if workers > 1:
            pool = pathos.pools.ProcessPool(nodes=workers)
            results = pool.imap(execute_task, queue)
        else:
            results = map(execute_task, queue)
        return list(results) 

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
                option['alpha'],
                train_inputs,
                train_response
            )
        elif option['type'] == 'svr':
            return self.try_svm(
                option['kernel'],
                option['degree'],
                option['alpha'],
                train_inputs,
                train_response
            )
        elif option['type'] == 'tree':
            return self.try_tree(
                option['depth'],
                train_inputs,
                train_response
            )
        elif option['type'] == 'adaboost':
            return self.try_ada(
                option['depth'],
                option['estimators'],
                train_inputs,
                train_response
            )
        elif option['type'] == 'random forest':
            return self.try_forest(
                option['depth'],
                option['estimators'],
                option['max_features'],
                train_inputs,
                train_response
            )
        else:
            raise RuntimeError('Unrecognized option type ' + option['type'])

    def try_linear(self, alpha, train_inputs, train_response):
        model = sklearn.linear_model.Ridge(alpha=alpha)
        model.fit(train_inputs, train_response)
        
        ret_val = {
            'model': model,
            'alpha': alpha,
            'type': 'linear'
        }
        
        return ret_val

    def try_svm(self, kernel, degree, alpha, train_inputs, train_response):
        model = sklearn.pipeline.Pipeline([
            ('scale', sklearn.preprocessing.StandardScaler()),
            ('svr', sklearn.svm.SVR(kernel=kernel, degree=degree, C=1-alpha))
        ])
        model.fit(train_inputs, train_response)
        
        ret_val = {
            'model': model,
            'kernel': kernel,
            'degree': degree,
            'alpha': alpha,
            'type': 'svr'
        }
        
        return ret_val

    def try_tree(self, depth, train_inputs, train_response):
        model = sklearn.tree.DecisionTreeRegressor(max_depth=depth)
        model.fit(train_inputs, train_response)
        
        ret_val = {
            'model': model,
            'depth': depth,
            'type': 'tree'
        }
        
        return ret_val

    def try_ada(self, depth, estimators, train_inputs, train_response):
        model_inner = sklearn.tree.DecisionTreeRegressor(max_depth=depth)
        model = sklearn.ensemble.AdaBoostRegressor(
            estimator=model_inner,
            n_estimators=estimators
        )
        model.fit(train_inputs, train_response)
        
        ret_val = {
            'model': model,
            'depth': depth,
            'estimators': estimators,
            'type': 'adaboost'
        }
        
        return ret_val

    def try_forest(self, depth, estimators, max_features, train_inputs, train_response):
        model = sklearn.ensemble.RandomForestRegressor(
            max_depth=depth,
            n_estimators=estimators,
            max_features=max_features
        )
        model.fit(train_inputs, train_response)
        
        ret_val = {
            'model': model,
            'depth': depth,
            'estimators': estimators,
            'type': 'random forest',
            'max_features': max_features
        }
        
        return ret_val

    def evaluate_target_set(self, predicted_collection, actual_collection):
        zipped = zip(predicted_collection, actual_collection)
        return statistics.mean(map(
            lambda x: self.evaluate_target_single(x[0], x[1]),
            zipped
        ))

    def is_out_sample_candidate(self, target):
        return target['beforeYear'] >= 2019 or target['afterYear'] >= 2019

    def is_out_sample_ignore(self, target):
        return target['beforeYear'] == 2020 or target['afterYear'] == 2020

    def evaluate_target_single(self, predicted, actuals):
        raise NotImplementedError('Use implementor.')

    def get_sql(self):
        raise NotImplementedError('Use implementor.')

    def get_cols(self):
        raise NotImplementedError('Use implementor.')
    
    def get_input_cols(self):
        raise NotImplementedError('Use implementor.')

    def get_response_col(self):
        raise NotImplementedError('Use implementor.')

    def get_output_cols(self):
        raise NotImplementedError('Use implementor.')

    def get_report_filename(self):
        raise NotImplementedError('Use implementor.')

    def get_model_filename(self):
        raise NotImplementedError('Use implementor.')

    def get_svm_enabled(self):
        raise NotImplementedError('Use implementor.')

    def get_model_class(self):
        raise NotImplementedError('Use implementor.')


class CheckSweepTask(luigi.Task):

    def run(self):
        with self.input().open('r') as f:
            job_info = json.load(f)

        force_type = job_info["forceModels"].get(self.get_model_class(), None)

        sweep_results_loc = os.path.join(
            job_info['directories']['output'],
            self.get_report_filename()
        )
        with open(sweep_results_loc) as f:
            reader = csv.DictReader(f)

            if force_type:
                allowed = filter(
                    lambda x: x['type'] == force_type,
                    reader
                )
            else:
                allowed = reader
            
            parsed_rows = map(lambda row: self.parse_row(row), allowed)
            best_model = min(parsed_rows, key=lambda x: x['validInSampleResponse'])

        self.check_model(best_model)

        with self.output().open('w') as f:
            return json.dump(job_info, f)

    def parse_row(self, row):
        return {
            'validInSampleTarget': float(row['validInSampleTarget']),
            'validOutSampleTarget': float(row['validOutSampleTarget']),
            'validInSampleResponse': float(row['validInSampleResponse']),
            'validOutSampleResponse': float(row['validOutSampleResponse']),
            'type': row['type']
        }

    def get_report_filename(self):
        raise NotImplementedError('Use implementor.')

    def check_model(self, target):
        raise NotImplementedError('Use implementor.')


class SweepConsumptionTask(SweepTask):

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return tasks_ml_prep.CheckMlConsumptionViewTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '300_sweep_consumption.json')
        return luigi.LocalTarget(out_path)

    def evaluate_target_single(self, predicted, actuals):
        actual_target = actuals[1]
        predicted_target = (1 + predicted) * actuals[0]
        return abs(actual_target - predicted_target)

    def get_sql(self):
        return '''
            SELECT
                afterYear,
                beforeYear,
                years,
                popChange,
                gdpPerCapChange,
                flagChina,
                flagEU30,
                flagNafta,
                flagRow,
                flagAgriculture,
                flagConstruction,
                flagElectronic,
                flagHouseholdLeisureSports,
                flagOther,
                flagPackaging,
                flagTextile,
                flagTransporation,
                consumptionChange,
                beforeConsumptionMT,
                afterConsumptionMT
            FROM
                instance_consumption_displaced
        '''

    def get_cols(self):
        return [
            'afterYear',
            'beforeYear',
            'years',
            'popChange',
            'gdpChange',
            'flagChina',
            'flagEU30',
            'flagNafta',
            'flagRow',
            'flagAgriculture',
            'flagConstruction',
            'flagElectronic',
            'flagHouseholdLeisureSports',
            'flagOther',
            'flagPackaging',
            'flagTextile',
            'flagTransporation',
            'consumptionChange',
            'beforeConsumptionMT',
            'afterConsumptionMT'
        ]
    
    def get_input_cols(self):
        return [
            'years',
            'popChange',
            'gdpChange',
            'flagChina',
            'flagEU30',
            'flagNafta',
            'flagRow',
            'flagAgriculture',
            'flagConstruction',
            'flagElectronic',
            'flagHouseholdLeisureSports',
            'flagOther',
            'flagPackaging',
            'flagTextile',
            'flagTransporation'
        ]

    def get_response_col(self):
        return 'consumptionChange'

    def get_output_cols(self):
        return [
            'beforeConsumptionMT',
            'afterConsumptionMT'
        ]

    def get_report_filename(self):
        return 'consumption_sweep.csv'

    def get_model_filename(self):
        return 'consumption.pickle'

    def get_svm_enabled(self):
        return True

    def get_model_class(self):
        return 'consumption'


class SweepWasteTask(SweepTask):

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return tasks_ml_prep.CheckMlConsumptionViewTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '301_sweep_waste.json')
        return luigi.LocalTarget(out_path)

    def evaluate_target_single(self, predicted, actuals):
        actual_target = actuals[0]
        return abs(actual_target - predicted)

    def get_sql(self):
        return '''
            SELECT
                afterYear,
                beforeYear,
                years,
                popChange,
                gdpPerCapChange,
                beforePercent,
                percentChange,
                flagChina,
                flagEU30,
                flagNafta,
                flagRow,
                flagRecycling,
                flagIncineration,
                flagLandfill,
                flagMismanaged,
                afterPercent
            FROM
                instance_waste_displaced
            WHERE
                beforePercent > 0
        '''

    def get_cols(self):
        return [
            'afterYear',
            'beforeYear',
            'years',
            'popChange',
            'gdpChange',
            'beforePercent',
            'percentChange',
            'flagChina',
            'flagEU30',
            'flagNafta',
            'flagRow',
            'flagRecycling',
            'flagIncineration',
            'flagLandfill',
            'flagMismanaged',
            'afterPercent'
        ]
    
    def get_input_cols(self):
        return [
            'years',
            'popChange',
            'gdpChange',
            'beforePercent',
            'flagChina',
            'flagEU30',
            'flagNafta',
            'flagRow',
            'flagRecycling',
            'flagIncineration',
            'flagLandfill',
            'flagMismanaged',
        ]

    def get_response_col(self):
        return 'afterPercent'

    def get_output_cols(self):
        return ['afterPercent']

    def get_report_filename(self):
        return 'waste_sweep.csv'

    def get_model_filename(self):
        return 'waste.pickle'

    def get_svm_enabled(self):
        return True

    def get_model_class(self):
        return 'waste'


class SweepTradeTask(SweepTask):

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return tasks_ml_prep.CheckMlTradeViewTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '302_sweep_trade.json')
        return luigi.LocalTarget(out_path)

    def evaluate_target_single(self, predicted, actuals):
        actual_target = actuals[1]
        predicted_target = (1 + predicted) * actuals[0]
        return abs(actual_target - predicted_target)

    def get_sql(self):
        return '''
            SELECT
                beforeYear,
                afterYear,
                years,
                popChange,
                gdpPerCapChange,
                flagChina,
                flagEU30,
                flagNafta,
                flagRow,
                flagArticles,
                flagFibers,
                flagGoods,
                flagResin,
                netMTChange,
                beforeNetMT,
                afterNetMT
            FROM
                instance_trade_displaced
        '''

    def get_cols(self):
        return [
            'beforeYear',
            'afterYear',
            'years',
            'popChange',
            'gdpChange',
            'flagChina',
            'flagEU30',
            'flagNafta',
            'flagRow',
            'flagArticles',
            'flagFibers',
            'flagGoods',
            'flagResin',
            'netMTChange',
            'beforeNetMT',
            'afterNetMT'
        ]
    
    def get_input_cols(self):
        return [
            'years',
            'popChange',
            'gdpChange',
            'flagChina',
            'flagEU30',
            'flagNafta',
            'flagRow',
            'flagArticles',
            'flagFibers',
            'flagGoods',
            'flagResin',
        ]

    def get_response_col(self):
        return 'netMTChange'

    def get_output_cols(self):
        return [
            'beforeNetMT',
            'afterNetMT'
        ]

    def get_report_filename(self):
        return 'trade_sweep.csv'

    def get_model_filename(self):
        return 'trade.pickle'

    def get_svm_enabled(self):
        return True

    def get_model_class(self):
        return 'trade'


class CheckSweepConsumptionTask(CheckSweepTask):

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return SweepConsumptionTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '303_check_consumption.json')
        return luigi.LocalTarget(out_path)

    def get_report_filename(self):
        return 'consumption_sweep.csv'

    def check_model(self, target):
        assert target['validOutSampleTarget'] < 2

    def get_model_class(self):
        return 'consumption'



class CheckSweepWasteTask(CheckSweepTask):

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return SweepWasteTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '304_check_waste.json')
        return luigi.LocalTarget(out_path)

    def get_report_filename(self):
        return 'waste_sweep.csv'

    def check_model(self, target):
        assert target['validOutSampleTarget'] < 0.02

    def get_model_class(self):
        return 'waste'


class CheckSweepTradeTask(CheckSweepTask):

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return SweepTradeTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '305_check_trade.json')
        return luigi.LocalTarget(out_path)

    def get_report_filename(self):
        return 'trade_sweep.csv'

    def check_model(self, target):
        assert target['validOutSampleTarget'] < 5

    def get_model_class(self):
        return 'trade'
