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

        self._estimate_and_add_region('china', cursor)
        self._estimate_and_add_region('eu30', cursor)
        self._estimate_and_add_region('nafta', cursor)
        self._estimate_and_add_region('row', cursor)

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


class ConfirmReadyTask(tasks_sql.SqlExecuteTask):
    """Target requiring everything is in place for adding history to primary."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return {
            'recent': RestructurePrimaryConsumptionTask(task_dir=self.task_dir),
            'historic': EstimateHistoricRegionalRecyclingTask(task_dir=self.task_dir)
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


class CreateWasteIntermediateTask(tasks_sql.SqlExecuteTask):
    """Create a table that will be modified inline that supports calculation of waste.

    Create a table that will be modified in-place that supports calculation of waste and,
    specifically, recycling.
    """

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return AddHistoryToPrimaryTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '016_waste_intermediate.json')
        return luigi.LocalTarget(out_path)

    def get_scripts(self):
        return [
            '04_secondary/create_intermediate_waste.sql'
        ]


class NormalizeForSecondaryTask(tasks_norm_lifecycle_template.NormalizeProjectionTask):
    """Normalize trade and EOL numbers ahead of lifecycle application."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return CreateWasteIntermediateTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '017_consumption_norm.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        return 'consumption_intermediate_waste'


class CheckNormalizeSecondaryTask(tasks_norm_lifecycle_template.NormalizeCheckTask):
    """Check normalization was successful on the secondary waste table."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return NormalizeForSecondaryTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '018_normalize_check.json')
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

    def requires(self):
        return CheckNormalizeSecondaryTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '019_lifecycle.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        return 'consumption_intermediate_waste'

    def get_start_year(self):
        return 1950

    def get_end_assert_year(self):
        return 2020

    def get_allowed_waste_waiting(self):
        return 5

    def get_end_year(self):
        return 2020


class SecondaryLifecycleCheckTask(tasks_norm_lifecycle_template.LifecycleCheckTask):
    """Check that lifecycle / lifetime distributions were applied to secondary artifact."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return ApplyLifecycleForSecondaryTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '020_check_lifecycle_ml.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        return 'consumption_intermediate_waste'


class SecondaryApplyWasteTradeTask(tasks_norm_lifecycle_template.ApplyWasteTradeProjectionTask):
    """Apply waste trade for secondary consumption goal."""
    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return SecondaryLifecycleCheckTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '021_secondary_apply_waste_trade.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        return 'consumption_intermediate_waste'


class AssignSecondaryConsumptionTask(tasks_sql.SqlExecuteTask):
    """Task which assigns recycled material to sectors."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return SecondaryApplyWasteTradeTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '022_sectorize_secondary.json')
        return luigi.LocalTarget(out_path)

    def get_scripts(self):
        return [
            '04_secondary/create_secondary_trade_pending.sql'
        ]


class CheckAssignSecondaryConsumptionTask(AssertEmptyTask):
    """Check that all of the recycling ended up accounted for."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return AssignSecondaryConsumptionTask(task_dir=self.task_dir)

    def get_script(self):
        return '04_secondary/check_allocation_region.sql'

    def output(self):
        out_path = os.path.join(self.task_dir, '023_check_sectorize_secondary.json')
        return luigi.LocalTarget(out_path)


class MoveProductionTradeSecondaryConsumptionTask(tasks_sql.SqlExecuteTask):
    """Task which takes care of secondary consumption through trade."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return CheckAssignSecondaryConsumptionTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '024_sectorize_secondary.json')
        return luigi.LocalTarget(out_path)

    def get_scripts(self):
        return [
            '04_secondary/move_traded_material.sql'
        ]


class CheckAssignSecondaryConsumptionTradeTask(AssertEmptyTask):
    """Check that all of the recycling ended up accounted for."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return MoveProductionTradeSecondaryConsumptionTask(task_dir=self.task_dir)

    def get_script(self):
        return '04_secondary/check_allocation_year.sql'

    def output(self):
        out_path = os.path.join(self.task_dir, '025_check_sectorize_secondary_trade.json')
        return luigi.LocalTarget(out_path)


class TemporallyDisplaceSecondaryConsumptionTask(tasks_sql.SqlExecuteTask):

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return CheckAssignSecondaryConsumptionTradeTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '026_temporal_displacement.json')
        return luigi.LocalTarget(out_path)

    def get_scripts(self):
        return [
            '04_secondary/displace_secondary_temporally.sql'
        ]


class RestructureSecondaryTask(tasks_sql.SqlExecuteTask):

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return TemporallyDisplaceSecondaryConsumptionTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '027_restructure_secondary.json')
        return luigi.LocalTarget(out_path)

    def get_scripts(self):
        return [
            '04_secondary/create_secondary_restructure.sql'
        ]


# TODO: Add to pending secondary
# TODO: Move into new primary restructure
# TODO: Clear immediate
# TODO: Repeat


class CombinePrimarySecondaryTask(tasks_sql.SqlExecuteTask):

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return RestructureSecondaryTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '032_combine_primary_secondary.json')
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
        out_path = os.path.join(self.task_dir, '033_check_combined_consumption.json')
        return luigi.LocalTarget(out_path)
