"""Tasks which run machine learning sweeps for models to predict trade, consumption, and waste.

License:
    BSD, see LICENSE.md
"""

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
    """Pickle-able model which includes logic to make input vectors."""

    def __init__(self, inner_model, cols):
        """Create a new model structure which can be pickled logic to make input vectors.

        Args:
            inner_model: The model to be decorated.
            cols: The list of column names in order that should be red to make input vectors.
        """
        self._inner_model = inner_model
        self._cols = cols

    def predict(self, targets):
        """Use the inner model to make a prediction.

        Args:
            targets: List of input tasks for the machine learning to perform where each dict in the
                collection has input values required by the model.

        Returns:
            The prediction.
        """
        targets_linear = [[target[col] for col in self._cols] for target in targets]
        return self._inner_model.predict(targets_linear)


class SweepTask(luigi.Task):
    """Template Method to build ML sweep tasks.

    Template Method to build Luigi Tasks that perform a machine learning sweep to create a model for
    a specific response variable like waste, consumption, or trade. These tasks yield a machine
    learning model as well as statistics about the models not chosen during the hyperparameter
    sweep.
    """

    def run(self):
        """Execute a sweep.


        Run a sweep over the available hyperparameter options for each machine learning model type
        available (linear, SVM, tree / CART, AdaBoost, Random Forest) and select a model to use in
        prediction. The final model, after evaluation, will be trained on all available data before
        moving forward into prediction.
        """
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
        """Load instances which will be used across the training, validation, and test sets.

        Args:
            job_info: the job JSON which describes where the instances can be found, specifically
                the scratch SQLite database.
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
        """Update instances in place to assign them to a training, validation, test, or ignore set.

        Assign input instances to test, train, and validation sets. Some instances may also be
        ignored if from an anamalous set like COVD / 2020.

        Args:
            instances: The instances (collection of dictionaries) to update. These will be modified
                in place, adding a setAssign and setAssignIn/OutSample to the instance.
        """
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
        """Perform the actual machine learning sweep.

        Args:
            instance: The instances with a setAssign attribute on which training and evaluation
                should happen.
            workers: The number of workers to run in parallel to complete the sweep. If 1, this is
                run as a normal Python execution. If more than 1, Pathos will be engaged to run
                in parallel.

        Returns:
            List of dictionaries with each dictionary describing a model attempted.
        """

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
        """Update model sweep records so they all have the same attributes.

        Different models will report different information about their hyperparameters such as
        kernel for SVR and max depth for trees. This will write empty values into the model records
        such that all records have the same set of attributes even if some are empty because they
        are not relevant for the model trained.

        Args:
            results: The results to update.

        Returns:
            Results after having standardized their keys.
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
        """Train a single model as part of the sweep.

        Args:
            option: Dictionary describing the model to be trained.
            train_inputs: The training instances on which the model should fit.
            train_response: The response values for each training instance such that train_response
                can pair with train_inputs in order.

        Returns:
            Dictionary describing the model fit along with hyperparameters. This will include the
            model iself under a "model" attribute.
        """
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
        """Try fitting a simple linear model.

        Args:
            alpha: Regularization parameter.
            train_inputs: The inputs on which the model should ber fit.
            train_response: The response variable to be predicted.

        Returns:
            Newly fit model.
        """
        model = sklearn.linear_model.Ridge(alpha=alpha)
        model.fit(train_inputs, train_response)

        ret_val = {
            'model': model,
            'alpha': alpha,
            'type': 'linear'
        }

        return ret_val

    def try_svm(self, kernel, degree, alpha, train_inputs, train_response):
        """Try support vector regression.

        Args:
            kernel: The name of the kernel like RBF to try.
            degree: The degree to use for SVR.
            alpha: Regularization parameter.
            train_inputs: The inputs on which the model should ber fit.
            train_response: The response variable to be predicted.

        Returns:
            Newly fit model.
        """
        model = sklearn.pipeline.Pipeline([
            ('scale', sklearn.preprocessing.StandardScaler()),
            ('svr', sklearn.svm.SVR(kernel=kernel, degree=degree, C=1 - alpha))
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
        """Try a decision tree regressor.

        Args:
            depth: The max depth allowed for the tree.
            train_inputs: The inputs on which the model should ber fit.
            train_response: The response variable to be predicted.

        Returns:
            Newly fit model.
        """
        model = sklearn.tree.DecisionTreeRegressor(max_depth=depth)
        model.fit(train_inputs, train_response)

        ret_val = {
            'model': model,
            'depth': depth,
            'type': 'tree'
        }

        return ret_val

    def try_ada(self, depth, estimators, train_inputs, train_response):
        """Try an AdaBoost regressor over a decision tree regressor.

        Args:
            depth: The max depth allowed for the tree.
            estimators: The number of trees allowed in the ensemble.
            train_inputs: The inputs on which the model should ber fit.
            train_response: The response variable to be predicted.

        Returns:
            Newly fit model.
        """
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
        """Try a Random Forest regressor.

        Args:
            depth: The max depth allowed for the tree.
            estimators: The number of trees allowed in the ensemble.
            max_features: The maximum allowed features (like log, sqrt, etc) per estimator.
            train_inputs: The inputs on which the model should ber fit.
            train_response: The response variable to be predicted.

        Returns:
            Newly fit model.
        """
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
        """Evaluate model performance on a specific set.

        Args:
            predicted_collection: The response variable / predictions from the model on this set.
            actual_collection: The response variable / actuals observed for this set such that this
                is in the same order to be paried with predicted_collection.

        Returns:
            Mean absolute error for this set.
        """
        zipped = zip(predicted_collection, actual_collection)
        return statistics.mean(map(
            lambda x: self.evaluate_target_single(x[0], x[1]),
            zipped
        ))

    def is_out_sample_candidate(self, target):
        """Determine if an instance should be labeled as out of sample.

        Determine if an instance is out of sample and should be set aside for the out of sample
        test.

        Args:
            target: The instance to be labeled.

        Returns:
            True if out of sample and False if in sample.
        """
        return target['beforeYear'] >= 2019 or target['afterYear'] >= 2019

    def is_out_sample_ignore(self, target):
        """Determine if an instance should be ignored.

        Args:
            target: The instance to be labeled.

        Returns:
            True if from an anamalous year (like COVID, 2020). False otherwise.
        """
        return target['beforeYear'] == 2020 or target['afterYear'] == 2020

    def evaluate_target_single(self, predicted, actuals):
        """Evaluate the error for a single prediction.

        Args:
            predicted: The model prediction.
            actuals: Vector of actual values required to evaluate a prediction.

        Returns:
            Error estimation
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

    def get_output_cols(self):
        """Get the columns required to evaluate a prediction.

        Get the columns required to evaluate a prediction which include the response variable but
        may include additional information required to convert the reponse to a meaningful error
        estimation. For example, a model predicting percent change may require the previous value as
        well to pair with its response.

        Returns:
            List of column names required to generate a useful error estimation.
        """
        raise NotImplementedError('Use implementor.')

    def get_report_filename(self):
        """Get the filename at which the sweep report should be written.

        Returns:
            The path to which the sweep report should be written.
        """
        raise NotImplementedError('Use implementor.')

    def get_model_filename(self):
        """Get the name of the file at which the model should be pickled.

        Returns:
            The path to which the model should be written.
        """
        raise NotImplementedError('Use implementor.')

    def get_svm_enabled(self):
        """Determine if support vector regression should be attempted in the sweep.

        Returns:
            True if SVM should be tried in the sweep and False otherwise.
        """
        raise NotImplementedError('Use implementor.')

    def get_model_class(self):
        """Get the type of model being trained.

        Returns:
            The type of model being trained like "consumption" which defines when it should be used.
        """
        raise NotImplementedError('Use implementor.')


class CheckSweepTask(luigi.Task):
    """Check if a sweep was successful."""

    def run(self):
        """Load a trained model and check that its sweep was successful."""
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
        """Parse a row of the sweep output describing a single model attempted.

        Args:
            row: The row to be parsed.

        Returns:
            The parsed row describing a model tried in the sweep.
        """
        return {
            'validInSampleTarget': float(row['validInSampleTarget']),
            'validOutSampleTarget': float(row['validOutSampleTarget']),
            'validInSampleResponse': float(row['validInSampleResponse']),
            'validOutSampleResponse': float(row['validOutSampleResponse']),
            'type': row['type']
        }

    def get_report_filename(self):
        """Get the name of the file at which the sweep report was written.

        Returns:
            The filename at which the sweep report can be read.
        """
        raise NotImplementedError('Use implementor.')

    def check_model(self, target):
        """Execute goal-specific model checks.

        Args:
            target: The record describing the chosen model from the sweep.
        """
        raise NotImplementedError('Use implementor.')


class SweepConsumptionTask(SweepTask):
    """Task to sweep for a consumption prediction model.

    Task to sweep for a consumption prediction model, predicting percent change in consumption for a
    sector from year to the next.
    """

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        """Require that the ML instance data have been generated."""
        return tasks_ml_prep.CheckMlConsumptionViewTask(task_dir=self.task_dir)

    def output(self):
        """Report that consumption was swept."""
        out_path = os.path.join(self.task_dir, '300_sweep_consumption.json')
        return luigi.LocalTarget(out_path)

    def evaluate_target_single(self, predicted, actuals):
        """Evaluate error of a percent change prediction from the swept model.

        Args:
            predicted: The predicted percent change.
            actuals: A two element vector where the first element is the prior consumption value
                and the second element is the actual value after the change.

        Returns:
            Estimated error for the percent change.
        """
        actual_target = actuals[1]
        predicted_target = (1 + predicted) * actuals[0]
        return abs(actual_target - predicted_target)

    def get_sql(self):
        """Get the SQL which is used to query for instances.

        Returns:
            SQL to retrieve instance which may be used for training, validation, or test.
        """
        return '''
            SELECT
                afterYear,
                beforeYear,
                years,
                popChange,
                gdpChange,
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
                flagTransportation,
                consumptionChange,
                beforeConsumptionMT,
                afterConsumptionMT
            FROM
                instance_consumption_displaced
        '''

    def get_cols(self):
        """Get the columns that will be returned from querying with get_sql.

        Returns:
            The in-order list of columns returned by the query described at get_sql.
        """
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
            'flagTransportation',
            'consumptionChange',
            'beforeConsumptionMT',
            'afterConsumptionMT'
        ]

    def get_input_cols(self):
        """Get the columns that that are inputs to the model.

        Returns:
            The list of columns in-order that are inputs into the model.
        """
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
            'flagTransportation'
        ]

    def get_response_col(self):
        """Get the column describing the response variable to be predicted.

        Returns:
            The name of the column in which the response variable can be found.
        """
        return 'consumptionChange'

    def get_output_cols(self):
        """Get the columns required to make an error estimation.

        Returns:
            List of column names indicating that consumption for the year in question and the
                consumption for the year prior are required to evaluate model performance.
        """
        return [
            'beforeConsumptionMT',
            'afterConsumptionMT'
        ]

    def get_report_filename(self):
        """Get the filename at which the sweep report should be written.

        Returns:
            The path to which the sweep report should be written.
        """
        return 'consumption_sweep.csv'

    def get_model_filename(self):
        """Get the name of the file at which the model should be pickled.

        Returns:
            The path to which the model should be written.
        """
        return 'consumption.pickle'

    def get_svm_enabled(self):
        """Indicate that SVM should be tried.

        Returns:
            True
        """
        return True

    def get_model_class(self):
        """Indicate that the model trained should be used for consumption.

        Returns:
            Tag for a consumption model.
        """
        return 'consumption'


class SweepWasteTask(SweepTask):
    """Task to sweep for a EOL fate propensity prediction model.

    Task to sweep for a EOL fate propensity prediction model, predicting the percent of waste
    generated for a year that will go to a specific fate.
    """

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        """Require that the ML instance data have been generated."""
        return tasks_ml_prep.CheckMlConsumptionViewTask(task_dir=self.task_dir)

    def output(self):
        """Report that waste fate propensity was swept."""
        out_path = os.path.join(self.task_dir, '301_sweep_waste.json')
        return luigi.LocalTarget(out_path)

    def evaluate_target_single(self, predicted, actuals):
        """Evaluate error of a propensity prediction from the swept model.

        Args:
            predicted: The propensity predicted.
            actuals: A single element vector with the actual fate propensity.

        Returns:
            Estimated error for the propensity.
        """
        actual_target = actuals[0]
        return abs(actual_target - predicted)

    def get_sql(self):
        """Get the SQL which is used to query for instances.

        Returns:
            SQL to retrieve instance which may be used for training, validation, or test.
        """
        return '''
            SELECT
                afterYear,
                beforeYear,
                years,
                popChange,
                gdpChange,
                beforePercent,
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
        """Get the columns that will be returned from querying with get_sql.

        Returns:
            The in-order list of columns returned by the query described at get_sql.
        """
        return [
            'afterYear',
            'beforeYear',
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
            'afterPercent'
        ]

    def get_input_cols(self):
        """Get the columns that that are inputs to the model.

        Returns:
            The list of columns in-order that are inputs into the model.
        """
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
        """Get the column describing the response variable to be predicted.

        Returns:
            The name of the column in which the response variable can be found.
        """
        return 'afterPercent'

    def get_output_cols(self):
        """Get the columns required to make a prediction.

        Returns:
            List of column names indicating that only fate propensity is required to evaluate model
            performance.
        """
        return ['afterPercent']

    def get_report_filename(self):
        """Get the filename at which the sweep report should be written.

        Returns:
            The path to which the sweep report should be written.
        """
        return 'waste_sweep.csv'

    def get_model_filename(self):
        """Get the name of the file at which the model should be pickled.

        Returns:
            The path to which the model should be written.
        """
        return 'waste.pickle'

    def get_svm_enabled(self):
        """Indicate that SVM should be tried.

        Returns:
            True
        """
        return True

    def get_model_class(self):
        """Indicate that the model trained should be used for waste fate propensity.

        Returns:
            Tag for a waste model.
        """
        return 'waste'


class SweepTradeTask(SweepTask):
    """Task to sweep for a goods and materials trade prediction model.

    Task to sweep for a goods and materials trade prediction model, predicting the ratio of trade
    (of a given type) to total consumption of the same year.
    """

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        """Require that the ML instance data have been generated."""
        return tasks_ml_prep.CheckMlTradeViewTask(task_dir=self.task_dir)

    def output(self):
        """Report that trade (goods / materials) was swept."""
        out_path = os.path.join(self.task_dir, '302_sweep_trade.json')
        return luigi.LocalTarget(out_path)

    def evaluate_target_single(self, predicted, actuals):
        """Evaluate error of a ratio prediction from the swept model.

        Evaluate error of a ratio prediction from the swept model. Note that this evaluates the
        MMT difference for historical reasons but future versions may switch to simply evaluating
        difference in the ratios directly.

        Args:
            predicted: The predicted ratio of goods / materials trade to consumption.
            actuals: A two element vector where the first element is the total consumption and the
                second is the amount of goods trade in the given type.

        Returns:
            Estimated error for the percent change.
        """
        actual_target = actuals[0] * actuals[1]
        predicted_target = predicted * actuals[0]
        return abs(actual_target - predicted_target)

    def get_sql(self):
        """Get the SQL which is used to query for instances.

        Returns:
            SQL to retrieve instance which may be used for training, validation, or test.
        """
        return '''
            SELECT
                beforeYear,
                afterYear,
                years,
                popChange,
                gdpChange,
                flagChina,
                flagEU30,
                flagNafta,
                flagRow,
                flagArticles,
                flagFibers,
                flagGoods,
                flagResin,
                beforePercent,
                afterPercent,
                beforeTotalConsumption,
                afterTotalConsumption
            FROM
                instance_trade_displaced
        '''

    def get_cols(self):
        """Get the columns that will be returned from querying with get_sql.

        Returns:
            The in-order list of columns returned by the query described at get_sql.
        """
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
            'beforePercent',
            'afterPercent',
            'beforeTotalConsumption',
            'afterTotalConsumption'
        ]

    def get_input_cols(self):
        """Get the columns that that are inputs to the model.

        Returns:
            The list of columns in-order that are inputs into the model.
        """
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
            'beforePercent'
        ]

    def get_response_col(self):
        """Get the column describing the response variable to be predicted.

        Returns:
            The name of the column in which the response variable can be found.
        """
        return 'afterPercent'

    def get_output_cols(self):
        """Get the columns required to make an error estimation.

        For historic reasons, this sweep asks for total consumption and trade ratio but future
        versions may only ask for trade ratio.

        Returns:
            For historic reasons, ask for total consumption and trade ratio.
        """
        return [
            'afterTotalConsumption',
            'afterPercent'
        ]

    def get_report_filename(self):
        """Get the filename at which the sweep report should be written.

        Returns:
            The path to which the sweep report should be written.
        """
        return 'trade_sweep.csv'

    def get_model_filename(self):
        """Get the name of the file at which the model should be pickled.

        Returns:
            The path to which the model should be written.
        """
        return 'trade.pickle'

    def get_svm_enabled(self):
        """Indicate that SVM should be tried.

        Returns:
            True
        """
        return True

    def get_model_class(self):
        """Indicate that the model trained should be used for goods and materials trade.

        Returns:
            Tag for a trade model.
        """
        return 'trade'


class SweepWasteTradeTask(SweepTask):
    """Task to sweep for a waste trade prediction model.

    Task to sweep for a waste trade prediction model, predicting the ratio of trade (of a given
    type) to total consumption of the same year.
    """

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        """Require that the ML instance data have been generated."""
        return tasks_ml_prep.CheckMlTradeViewTask(task_dir=self.task_dir)

    def output(self):
        """Report that trade (waste) was swept."""
        out_path = os.path.join(self.task_dir, '303_sweep_waste_trade.json')
        return luigi.LocalTarget(out_path)

    def evaluate_target_single(self, predicted, actuals):
        """Evaluate error of a ratio prediction from the swept model.

        Evaluate error of a ratio prediction from the swept model. Note that this evaluates the
        MMT difference for historical reasons but future versions may switch to simply evaluating
        difference in the ratios directly.

        Args:
            predicted: The predicted ratio of waste trade to consumption.
            actuals: A two element vector where the first element is the total consumption and the
                second is the amount of goods trade in the given type.

        Returns:
            Estimated error for the percent change.
        """
        actual_target = actuals[0] * actuals[1]
        predicted_target = predicted * actuals[0]
        return abs(actual_target - predicted_target)

    def get_sql(self):
        """Get the SQL which is used to query for instances.

        Returns:
            SQL to retrieve instance which may be used for training, validation, or test.
        """
        return '''
            SELECT
                beforeYear,
                afterYear,
                years,
                popChange,
                gdpChange,
                flagChina,
                flagEU30,
                flagNafta,
                flagRow,
                flagSword,
                beforePercent,
                afterPercent,
                beforeTotalConsumption,
                afterTotalConsumption
            FROM
                instance_waste_trade_displaced
            WHERE
                beforeYear >= 2007
                AND afterYear >= 2007
        '''

    def get_cols(self):
        """Get the columns that will be returned from querying with get_sql.

        Returns:
            The in-order list of columns returned by the query described at get_sql.
        """
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
            'flagSword',
            'beforePercent',
            'afterPercent',
            'beforeTotalConsumption',
            'afterTotalConsumption'
        ]

    def get_input_cols(self):
        """Get the columns that that are inputs to the model.

        Returns:
            The list of columns in-order that are inputs into the model.
        """
        return [
            'years',
            'popChange',
            'gdpChange',
            'flagChina',
            'flagEU30',
            'flagNafta',
            'flagRow',
            'flagSword',
            'beforePercent'
        ]

    def get_response_col(self):
        """Get the column describing the response variable to be predicted.

        Returns:
            The name of the column in which the response variable can be found.
        """
        return 'afterPercent'

    def get_output_cols(self):
        """Get the columns required to make an error estimation.

        For historic reasons, this sweep asks for total consumption and trade ratio but future
        versions may only ask for trade ratio.

        Returns:
            For historic reasons, ask for total consumption and trade ratio.
        """
        return [
            'afterTotalConsumption',
            'afterPercent'
        ]

    def get_report_filename(self):
        """Get the filename at which the sweep report should be written.

        Returns:
            The path to which the sweep report should be written.
        """
        return 'wasteTrade_sweep.csv'

    def get_model_filename(self):
        """Get the name of the file at which the model should be pickled.

        Returns:
            The path to which the model should be written.
        """
        return 'wasteTrade.pickle'

    def get_svm_enabled(self):
        """Indicate that SVM should be tried.

        Returns:
            True
        """
        return True

    def get_model_class(self):
        """Indicate that the model trained should be used for waste trade.

        Returns:
            Tag for a trade model.
        """
        return 'wasteTrade'


class CheckSweepConsumptionTask(CheckSweepTask):
    """Check the machine learning model found for consumption prediction."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        """Require the consumption sweep have happened."""
        return SweepConsumptionTask(task_dir=self.task_dir)

    def output(self):
        """Report that the sweep passed checks."""
        out_path = os.path.join(self.task_dir, '304_check_consumption.json')
        return luigi.LocalTarget(out_path)

    def get_report_filename(self):
        """Provide a filename where the sweep results can be found."""
        return 'consumption_sweep.csv'

    def check_model(self, target):
        """Assert validation out of sample performance threshold met."""
        assert target['validOutSampleTarget'] < 2

    def get_model_class(self):
        """Indicate that the consumption model is being checked."""
        return 'consumption'


class CheckSweepWasteTask(CheckSweepTask):
    """Check the machine learning model found for waste EOL fate propensity prediction."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        """Require the waste sweep have happened."""
        return SweepWasteTask(task_dir=self.task_dir)

    def output(self):
        """Report that the sweep passed checks."""
        out_path = os.path.join(self.task_dir, '305_check_waste.json')
        return luigi.LocalTarget(out_path)

    def get_report_filename(self):
        """Provide a filename where the sweep results can be found."""
        return 'waste_sweep.csv'

    def check_model(self, target):
        """Assert validation out of sample performance threshold met."""
        assert target['validOutSampleTarget'] < 0.02

    def get_model_class(self):
        """Indicate that the waste model is being checked."""
        return 'waste'


class CheckSweepTradeTask(CheckSweepTask):
    """Check the machine learning model found for goods and materials trade prediction."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        """Require the trade (goods / materials) sweep have happened."""
        return SweepTradeTask(task_dir=self.task_dir)

    def output(self):
        """Report that the sweep passed checks."""
        out_path = os.path.join(self.task_dir, '306_check_trade.json')
        return luigi.LocalTarget(out_path)

    def get_report_filename(self):
        """Provide a filename where the sweep results can be found."""
        return 'trade_sweep.csv'

    def check_model(self, target):
        """Assert validation out of sample performance threshold met."""
        assert target['validOutSampleTarget'] < 4

    def get_model_class(self):
        """Indicate that the trade (goods / materials) model is being checked."""
        return 'trade'


class CheckSweepWasteTradeTask(CheckSweepTask):
    """Check the machine learning model found for waste trade prediction."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        """Require the trade (waste) sweep have happened."""
        return SweepWasteTradeTask(task_dir=self.task_dir)

    def output(self):
        """Report that the sweep passed checks."""
        out_path = os.path.join(self.task_dir, '307_check_waste_trade.json')
        return luigi.LocalTarget(out_path)

    def get_report_filename(self):
        """Provide a filename where the sweep results can be found."""
        return 'wasteTrade_sweep.csv'

    def check_model(self, target):
        """Assert validation out of sample performance threshold met."""
        assert target['validOutSampleTarget'] < 4

    def get_model_class(self):
        """Indicate that the trade (waste) model is being checked."""
        return 'wasteTrade'
