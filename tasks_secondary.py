"""Tasks which estimate secondary production.

@license BSD, see README.md
"""
import json
import os
import sqlite3

import luigi
import sklearn.linear_model

import const
import tasks_norm_lifecycle_template
import tasks_preprocess
import tasks_sql


class AssertEmptyTask(luigi.Task):
    """Template method for checking that a count is zero."""

    def run(self):
        """Execute the check which, by default, simply confirms that the table is non-empty."""
        with self.input().open('r') as f:
            job_info = json.load(f)

        database_loc = job_info['database']
        connection = sqlite3.connect(database_loc)
        cursor = connection.cursor()

        path_pieces = self.get_script().split('/')
        full_path = os.path.join(*([const.SQL_DIR] + path_pieces))
        with open(full_path) as f:
            query = f.read()

        cursor.execute(query)
        results = cursor.fetchall()
        assert results[0][0] == 0

        connection.commit()

        cursor.close()
        connection.close()

        with self.output().open('w') as f:
            return json.dump(job_info, f)


class RestructurePrimaryConsumptionTask(tasks_sql.SqlExecuteTask):
    """Task which restructures primary consumption data.

    Task which restructures primary consumption data with supporting data available as required for
    lifecycle distributions.
    """

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return tasks_preprocess.BuildViewsTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '010_primary_consumption_restructure.json')
        return luigi.LocalTarget(out_path)

    def get_scripts(self):
        return [
            '04_secondary/create_primary_restructure.sql'
        ]


class MakeHistoricRecyclingTableTask(tasks_sql.SqlExecuteTask):
    """Task which makes an empty table for historic recycling rates."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return tasks_preprocess.BuildViewsTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '011_make_historic_recycling_table.json')
        return luigi.LocalTarget(out_path)

    def get_scripts(self):
        return [
            '04_secondary/create_historic_recycling_empty.sql'
        ]


class EstimateHistoricRegionalRecyclingTask(tasks_sql.SqlExecuteTask):
    """Task which uses simple linear regression to estimate historic recycling rates."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return MakeHistoricRecyclingTableTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '012_estimate_historic.json')
        return luigi.LocalTarget(out_path)

    def run(self):
        with self.input().open('r') as f:
            job_info = json.load(f)

        database_loc = job_info['database']
        connection = sqlite3.connect(database_loc)
        cursor = connection.cursor()

        for region in const.REGIONS:
            self._estimate_and_add_region(region, cursor)

        connection.commit()

        cursor.close()
        connection.close()

        with self.output().open('w') as f:
            return json.dump(job_info, f)

    def _estimate_and_add_region(self, region, cursor):
        cursor.execute(
            '''
            SELECT
                year,
                percent
            FROM
                eol
            WHERE
                region = ?
                AND type = 'recycling'
            ''',
            (region,)
        )
        training = cursor.fetchall()

        model = sklearn.linear_model.LinearRegression()
        model.fit(
            [[int(x[0])] for x in training],
            [float(x[1]) for x in training],
        )

        output_years = range(1950, 2005)
        output_records_uncapped = map(lambda year: {
            'year': year,
            'percent': model.predict([[year]])[0]
        }, output_years)
        output_records = map(lambda record: {
            'year': record['year'],
            'percent': record['percent'] if record['percent'] > 0 else 0
        }, output_records_uncapped)
        output_records_flat = map(
            lambda x: [x['year'], region, 'recycling', x['percent']],
            output_records
        )

        cursor.executemany(
            '''
            INSERT INTO historic_recycling_estimate (year, region, type, percent) VALUES (
                ?,
                ?,
                ?,
                ?
            )
            ''',
            output_records_flat
        )


class SeedPendingSecondaryTask(tasks_sql.SqlExecuteTask):
    """Create a table to accumulate secondary consumption."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return tasks_preprocess.BuildViewsTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '013_seed_pending_secondary.json')
        return luigi.LocalTarget(out_path)

    def get_scripts(self):
        return [
            '04_secondary/seed_pending_secondary.sql'
        ]


class ConfirmReadyTask(luigi.Task):
    """Target requiring everything is in place for adding history to primary."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return {
            'recent': RestructurePrimaryConsumptionTask(task_dir=self.task_dir),
            'historic': EstimateHistoricRegionalRecyclingTask(task_dir=self.task_dir),
            'pending': SeedPendingSecondaryTask(task_dir=self.task_dir)
        }

    def output(self):
        out_path = os.path.join(self.task_dir, '014_confirm_ready.json')
        return luigi.LocalTarget(out_path)

    def run(self):
        """Execute the check which, by default, simply confirms that the table is non-empty."""
        with self.input()['historic'].open('r') as f:
            job_info = json.load(f)

        database_loc = job_info['database']
        connection = sqlite3.connect(database_loc)
        cursor = connection.cursor()

        cursor.execute('SELECT count(1) FROM historic_recycling_estimate')
        results = cursor.fetchall()
        assert results[0][0] > 0

        connection.commit()

        cursor.close()
        connection.close()

        with self.output().open('w') as f:
            return json.dump(job_info, f)


class AddHistoryToPrimaryTask(tasks_sql.SqlExecuteTask):
    """Add records for primary consumption prior to 2005."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return ConfirmReadyTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '015_add_history.json')
        return luigi.LocalTarget(out_path)

    def get_scripts(self):
        return [
            '04_secondary/add_history_to_primary.sql'
        ]


class ConfirmIterationReadyTask(luigi.Task):
    """Target requiring all iterations of circularity are complete, checking table populated."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    iteration = luigi.Parameter(default=0)

    def requires(self):
        iteration_int = int(self.iteration)
        if iteration_int > 0:
            return {
                'history': AddHistoryToPrimaryTask(task_dir=self.task_dir),
                'iteration': CompleteIterationTask(
                    task_dir=self.task_dir,
                    iteration=str(iteration_int - 1)
                )
            }
        else:
            return {
                'history': AddHistoryToPrimaryTask(task_dir=self.task_dir)
            }

    def output(self):
        out_path = os.path.join(self.task_dir, '016_%s_start_iteration.json' % self.iteration)
        return luigi.LocalTarget(out_path)

    def run(self):
        with self.input()['history'].open('r') as f:
            job_info = json.load(f)

        with self.output().open('w') as f:
            return json.dump(job_info, f)


class CreateWasteIntermediateTask(tasks_sql.SqlExecuteTask):
    """Create a table that will be modified inline that supports calculation of waste.

    Create a table that will be modified in-place that supports calculation of waste and,
    specifically, recycling.
    """

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    iteration = luigi.Parameter(default=0)

    def requires(self):
        return ConfirmIterationReadyTask(task_dir=self.task_dir, iteration=self.iteration)

    def output(self):
        out_path = os.path.join(self.task_dir, '017_%s_waste_intermediate.json' % self.iteration)
        return luigi.LocalTarget(out_path)

    def get_scripts(self):
        return [
            '04_secondary/create_intermediate_waste.sql'
        ]


class NormalizeForSecondaryTask(tasks_norm_lifecycle_template.NormalizeProjectionTask):
    """Normalize trade and EOL numbers ahead of lifecycle application."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    iteration = luigi.Parameter(default=0)

    def requires(self):
        return CreateWasteIntermediateTask(task_dir=self.task_dir, iteration=self.iteration)

    def output(self):
        out_path = os.path.join(self.task_dir, '018_%s_consumption_norm.json' % self.iteration)
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        return 'consumption_intermediate_waste'


class CheckNormalizeSecondaryTask(tasks_norm_lifecycle_template.NormalizeCheckTask):
    """Check normalization was successful on the secondary waste table."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    iteration = luigi.Parameter(default=0)

    def requires(self):
        return NormalizeForSecondaryTask(task_dir=self.task_dir, iteration=self.iteration)

    def output(self):
        out_path = os.path.join(self.task_dir, '019_%s_normalize_check.json' % self.iteration)
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        return 'consumption_intermediate_waste'

    def should_assert_waste_trade_min(self):
        return True

    def should_assert_trade_max(self):
        return True


class ApplyLifecycleForSecondaryTask(tasks_norm_lifecycle_template.ApplyLifecycleTask):
    """Apply the lifecycles for the purposes of determining secondary consumption."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    iteration = luigi.Parameter(default=0)

    def requires(self):
        return CheckNormalizeSecondaryTask(task_dir=self.task_dir, iteration=self.iteration)

    def output(self):
        out_path = os.path.join(self.task_dir, '020_%s_lifecycle.json' % self.iteration)
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        return 'consumption_intermediate_waste'

    def get_start_year(self):
        return 1950 + int(self.iteration)

    def get_end_assert_year(self):
        return 2020

    def get_allowed_waste_waiting(self):
        return 5

    def get_end_year(self):
        return 2020


class SecondaryLifecycleCheckTask(tasks_norm_lifecycle_template.LifecycleCheckTask):
    """Check that lifecycle / lifetime distributions were applied to secondary artifact."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    iteration = luigi.Parameter(default=0)

    def requires(self):
        return ApplyLifecycleForSecondaryTask(task_dir=self.task_dir, iteration=self.iteration)

    def output(self):
        out_path = os.path.join(
            self.task_dir,
            '021_%s_check_lifecycle_ml.json' % self.iteration
        )
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        return 'consumption_intermediate_waste'


class SecondaryApplyWasteTradeTask(tasks_norm_lifecycle_template.ApplyWasteTradeProjectionTask):
    """Apply waste trade for secondary consumption goal."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    iteration = luigi.Parameter(default=0)

    def requires(self):
        return SecondaryLifecycleCheckTask(task_dir=self.task_dir, iteration=self.iteration)

    def output(self):
        out_path = os.path.join(
            self.task_dir,
            '022_%s_secondary_apply_waste_trade.json' % self.iteration
        )
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        return 'consumption_intermediate_waste'


class AssignSecondaryConsumptionTask(tasks_sql.SqlExecuteTask):
    """Task which assigns recycled material to sectors."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    iteration = luigi.Parameter(default=0)

    def requires(self):
        return SecondaryApplyWasteTradeTask(task_dir=self.task_dir, iteration=self.iteration)

    def output(self):
        out_path = os.path.join(
            self.task_dir,
            '023_%s_sectorize_secondary.json' % self.iteration
        )
        return luigi.LocalTarget(out_path)

    def get_scripts(self):
        return [
            '04_secondary/create_secondary_trade_pending.sql'
        ]


class CheckAssignSecondaryConsumptionTask(AssertEmptyTask):
    """Check that all of the recycling ended up accounted for."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    iteration = luigi.Parameter(default=0)

    def requires(self):
        return AssignSecondaryConsumptionTask(task_dir=self.task_dir, iteration=self.iteration)

    def get_script(self):
        return '04_secondary/check_allocation_region.sql'

    def output(self):
        out_path = os.path.join(
            self.task_dir,
            '024_%s_check_sectorize_secondary.json' % self.iteration
        )
        return luigi.LocalTarget(out_path)


class MoveProductionTradeSecondaryConsumptionTask(tasks_sql.SqlExecuteTask):
    """Task which takes care of secondary consumption through trade."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    iteration = luigi.Parameter(default=0)

    def requires(self):
        return CheckAssignSecondaryConsumptionTask(task_dir=self.task_dir, iteration=self.iteration)

    def output(self):
        out_path = os.path.join(self.task_dir, '025_%s_sectorize_secondary.json' % self.iteration)
        return luigi.LocalTarget(out_path)

    def get_scripts(self):
        return [
            '04_secondary/move_traded_material.sql'
        ]


class CheckAssignSecondaryConsumptionTradeTask(AssertEmptyTask):
    """Check that all of the recycling ended up accounted for."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    iteration = luigi.Parameter(default=0)

    def requires(self):
        return MoveProductionTradeSecondaryConsumptionTask(
            task_dir=self.task_dir,
            iteration=self.iteration
        )

    def get_script(self):
        return '04_secondary/check_allocation_year.sql'

    def output(self):
        out_path = os.path.join(
            self.task_dir,
            '026_%s_check_sectorize_secondary_trade.json' % self.iteration
        )
        return luigi.LocalTarget(out_path)


class TemporallyDisplaceSecondaryConsumptionTask(tasks_sql.SqlExecuteTask):
    """Add delay for recycling to re-enter production."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    iteration = luigi.Parameter(default=0)

    def requires(self):
        return CheckAssignSecondaryConsumptionTradeTask(
            task_dir=self.task_dir,
            iteration=self.iteration
        )

    def output(self):
        out_path = os.path.join(self.task_dir, '027_%s_temporal_displacement.json' % self.iteration)
        return luigi.LocalTarget(out_path)

    def get_scripts(self):
        return [
            '04_secondary/displace_secondary_temporally.sql'
        ]


class RestructureSecondaryTask(tasks_sql.SqlExecuteTask):
    """Restructure the output to match the expected inputs of downstream."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    iteration = luigi.Parameter(default=0)

    def requires(self):
        return TemporallyDisplaceSecondaryConsumptionTask(
            task_dir=self.task_dir,
            iteration=self.iteration
        )

    def output(self):
        out_path = os.path.join(self.task_dir, '028_%s_restructure_secondary.json' % self.iteration)
        return luigi.LocalTarget(out_path)

    def get_scripts(self):
        return [
            '04_secondary/create_secondary_restructure.sql'
        ]


class AddToPendingSecondaryTask(tasks_sql.SqlExecuteTask):
    """Accumulate the new values into the pending table."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    iteration = luigi.Parameter(default=0)

    def requires(self):
        return RestructureSecondaryTask(task_dir=self.task_dir, iteration=self.iteration)

    def output(self):
        out_path = os.path.join(self.task_dir, '029_%s_add_to_pending.json' % self.iteration)
        return luigi.LocalTarget(out_path)

    def get_scripts(self):
        return [
            '04_secondary/add_to_pending.sql'
        ]


class ClearPriorInputsTask(tasks_sql.SqlExecuteTask):
    """Clear the restructured inputs table."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    iteration = luigi.Parameter(default=0)

    def requires(self):
        return AddToPendingSecondaryTask(task_dir=self.task_dir, iteration=self.iteration)

    def output(self):
        out_path = os.path.join(self.task_dir, '030_%s_clear_inputs.json' % self.iteration)
        return luigi.LocalTarget(out_path)

    def get_scripts(self):
        return [
            '04_secondary/clear_input.sql'
        ]


class RecirculateInputsTask(tasks_sql.SqlExecuteTask):
    """Change inputs for next iteration."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    iteration = luigi.Parameter(default=0)

    def requires(self):
        return ClearPriorInputsTask(task_dir=self.task_dir, iteration=self.iteration)

    def output(self):
        out_path = os.path.join(self.task_dir, '031_%s_recirculate_inputs.json' % self.iteration)
        return luigi.LocalTarget(out_path)

    def get_scripts(self):
        return [
            '04_secondary/recirculate_secondary.sql'
        ]


class CompleteIterationTask(tasks_sql.SqlExecuteTask):
    """Require loop and reconstruction of the intermediate artifacts before looping again."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    iteration = luigi.Parameter(default=0)

    def requires(self):
        return RecirculateInputsTask(task_dir=self.task_dir, iteration=self.iteration)

    def output(self):
        out_path = os.path.join(self.task_dir, '032_%s_clear_intermediate.json' % self.iteration)
        return luigi.LocalTarget(out_path)

    def get_scripts(self):
        return [
            '04_secondary/clear_intermediate_waste.sql',
            '04_secondary/clear_intermediate_pre_trade.sql',
            '04_secondary/clear_intermediate_post_trade.sql',
            '04_secondary/clear_intermediate_displaced.sql',
            '04_secondary/clear_intermediate_output.sql'
        ]


class ConfirmIterationsTask(luigi.Task):
    """Target requiring all iterations of circularity are complete, checking table populated."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return CompleteIterationTask(task_dir=self.task_dir, iteration=const.CIRCULARITY_LOOPS)

    def output(self):
        out_path = os.path.join(self.task_dir, '033_confirm_iterations.json')
        return luigi.LocalTarget(out_path)

    def run(self):
        with self.input().open('r') as f:
            job_info = json.load(f)

        database_loc = job_info['database']
        connection = sqlite3.connect(database_loc)
        cursor = connection.cursor()

        cursor.execute('SELECT count(1) FROM consumption_secondary_pending WHERE consumptionMT > 0')
        results = cursor.fetchall()
        assert results[0][0] > 0

        connection.commit()

        cursor.close()
        connection.close()

        with self.output().open('w') as f:
            return json.dump(job_info, f)


class CombinePrimarySecondaryTask(tasks_sql.SqlExecuteTask):

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return ConfirmIterationsTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '034_combine_primary_secondary.json')
        return luigi.LocalTarget(out_path)

    def get_scripts(self):
        return [
            '04_secondary/combine_primary_secondary.sql'
        ]


class CheckCombinedConsumptionTask(AssertEmptyTask):
    """Check for nulls."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return CombinePrimarySecondaryTask(task_dir=self.task_dir)

    def get_script(self):
        return '04_secondary/check_combined_consumption.sql'

    def output(self):
        out_path = os.path.join(self.task_dir, '035_check_combined_consumption.json')
        return luigi.LocalTarget(out_path)
