"""Tasks which estimate secondary production.

@license BSD, see README.md
"""
import os

import luigi

import tasks_norm_lifecycle_template
import tasks_preprocess
import tasks_sql

import const


class RestructurePrimaryConsumptionTask(tasks_sql.SqlExecuteTask):
    """Task which restructures primary consumption data.

    Task which restructures primary consumption data with supporting data available as required for
    lifecycle distributions.
    """

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        """Require that the auxiliary data have been confirmed present."""
        return tasks_preprocess.BuildViewsTask(task_dir=self.task_dir)

    def output(self):
        """Report that the views for working with auxiliary data have been built."""
        out_path = os.path.join(self.task_dir, '010_primary_consumption_restructure.json')
        return luigi.LocalTarget(out_path)

    def get_scripts(self):
        """Get the location of scripts needed to build the auxiliary data views."""
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
        """Require that the auxiliary data have been confirmed present."""
        return RestructurePrimaryConsumptionTask(task_dir=self.task_dir)

    def output(self):
        """Report that the views for working with auxiliary data have been built."""
        out_path = os.path.join(self.task_dir, '011_waste_intermediate.json')
        return luigi.LocalTarget(out_path)

    def get_scripts(self):
        """Get the location of scripts needed to build the auxiliary data views."""
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


class ApplyLifecycleForSecondaryTask(tasks_norm_lifecycle_template.ApplyLifecycleTask):
    """Apply the lifecycles for the purposes of determining secondary consumption."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return NormalizeForSecondaryTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '013_lifecycle.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        return 'consumption_intermediate_waste'


# class SecondaryApplyWasteTradeProjectionTask(
#     tasks_norm_lifecycle_template.ApplyWasteTradeProjectionTask):
#     """Apply waste trade for secondary consumption goal."""
#
#     task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
#
#     def requires(self):
#         return MlLifecycleCheckTask(task_dir=self.task_dir)
#
#     def output(self):
#         out_path = os.path.join(self.task_dir, '012_secondary_apply_waste_trade.json')
#         return luigi.LocalTarget(out_path)
#
#     def get_table_name(self):
#         return 'project_ml'
# NormalizeCheckTask
# LifecycleCheckTask
