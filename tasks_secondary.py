"""Tasks which estimate secondary production.

@license BSD, see README.md
"""
import os

import luigi

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
