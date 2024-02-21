"""Tasks which estimate secondary production.

@license BSD, see README.md
"""
import json
import os
import sqlite3

import luigi

import const
import tasks_norm_lifecycle_template
import tasks_preprocess
import tasks_sql


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


class CreateWasteIntermediateTask(tasks_sql.SqlExecuteTask):
    """Create a table that will be modified inline that supports calculation of waste.

    Create a table that will be modified in-place that supports calculation of waste and,
    specifically, recycling.
    """

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return RestructurePrimaryConsumptionTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '011_waste_intermediate.json')
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
        out_path = os.path.join(self.task_dir, '012_consumption_norm.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        return 'consumption_intermediate_waste'


class CheckNormalizeSecondaryTask(tasks_norm_lifecycle_template.NormalizeCheckTask):
    """Check normalization was successful on the secondary waste table."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return NormalizeForSecondaryTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '013_normalize_check.json')
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
        out_path = os.path.join(self.task_dir, '014_lifecycle.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        return 'consumption_intermediate_waste'

    def get_start_year(self):
        return 2005

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
        out_path = os.path.join(self.task_dir, '015_check_lifecycle_ml.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        return 'consumption_intermediate_waste'


class SecondaryApplyWasteTradeTask(tasks_norm_lifecycle_template.ApplyWasteTradeProjectionTask):
    """Apply waste trade for secondary consumption goal."""
    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return SecondaryLifecycleCheckTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '016_secondary_apply_waste_trade.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        return 'consumption_intermediate_waste'


class AssignSecondaryConsumptionTask(tasks_sql.SqlExecuteTask):
    """Task which assigns recycled material to sectors."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return SecondaryApplyWasteTradeTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '017_sectorize_secondary.json')
        return luigi.LocalTarget(out_path)

    def get_scripts(self):
        return [
            '04_secondary/create_secondary_trade_pending.sql'
        ]


class CheckAssignSecondaryConsumptionTask(luigi.Task):
    """Template Method which checks that table or view has contents in it."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return AssignSecondaryConsumptionTask(task_dir=self.task_dir)
    
    def run(self):
        """Execute the check which, by default, simply confirms that the table is non-empty."""
        with self.input().open('r') as f:
            job_info = json.load(f)

        database_loc = job_info['database']
        connection = sqlite3.connect(database_loc)
        cursor = connection.cursor()

        path_pieces = '04_secondary/check_allocation.sql'.split('/')
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
    
    def output(self):
        out_path = os.path.join(self.task_dir, '018_check_sectorize_secondary.json')
        return luigi.LocalTarget(out_path)
