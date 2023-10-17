"""Tasks which prepare data before their use in a model sweep including machine learning.

Tasks which prepare data before their use in a model sweep including machine learning. Unlike
tasks_preprocess, these steps are not specific to individual modeling tasks.

License:
    BSD, see LICENSE.md
"""

import csv
import json
import os
import statistics
import sqlite3

import luigi

import const
import tasks_auxiliary
import tasks_preprocess
import tasks_sql


class CheckMlPrepTask(luigi.Task):
    """Check that view required for modeling data preparation have been satisfied."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        """Indicate that data preprocessing for plastics and auxiliary data are required."""
        return {
            'auxiliary': tasks_auxiliary.CheckViewsAuxTask(task_dir=self.task_dir),
            'preprocess': tasks_preprocess.CheckViewsTask(task_dir=self.task_dir)
        }

    def output(self):
        """Report that the prerequisite views have been checked."""
        out_path = os.path.join(self.task_dir, '200_check_prep.json')
        return luigi.LocalTarget(out_path)

    def run(self):
        """Run the check on the required views."""
        with self.input()['auxiliary'].open('r') as f:
            job_info = json.load(f)

        database_loc = job_info['database']
        connection = sqlite3.connect(database_loc)

        cursor = connection.cursor()
        cursor.execute('''
            SELECT
                gdp,
                population
            FROM
                auxiliary
            WHERE
                year = 2020
                AND region = 'nafta'
        ''')
        aux_results = cursor.fetchall()[0]
        assert aux_results[0] > 0
        assert aux_results[1] > 0

        cursor = connection.cursor()
        cursor.execute('''
            SELECT
                consumptionPackagingMT,
                eolRecyclingPercent
            FROM
                summary
            WHERE
                year = 2020
                AND region = 'nafta'
        ''')
        preprocess_results = cursor.fetchall()[0]
        assert preprocess_results[0] > 0
        assert preprocess_results[1] > 0

        cursor.close()
        connection.close()

        with self.output().open('w') as f:
            return json.dump(job_info, f)



class BuildMlViewsTask(tasks_sql.SqlExecuteTask):
    """Execute a series of scripts to build derivative views required for individual models.

    Build views through a series of scripts that build views deriving from other views that address
    the needs of specific modeling tasks ahead of sweeps.
    """
    
    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    
    def requires(self):
        """Require that the prerequisite views were checked."""
        return CheckMlPrepTask(task_dir=self.task_dir)

    def output(self):
        """Report that the task-specific views have been constructed."""
        out_path = os.path.join(self.task_dir, '201_build_views.json')
        return luigi.LocalTarget(out_path)

    def get_scripts(self):
        """Get the list of scripts that need to be executed."""
        return [
            '07_instance/instance_consumption_normal.sql',
            '07_instance/instance_consumption_displaced.sql',
            '07_instance/instance_waste_normal.sql',
            '07_instance/instance_waste_displaced.sql',
            '07_instance/instance_trade_normal.sql',
            '07_instance/instance_trade_displaced.sql',
            '07_instance/instance_waste_trade_normal.sql',
            '07_instance/instance_waste_trade_displaced.sql'
        ]


class CheckMlConsumptionViewTask(tasks_sql.SqlCheckTask):
    """Check that the task-specific view for consumption prediction was created successfully."""
    
    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    
    def requires(self):
        """Require that the task-specific views were built."""
        return BuildMlViewsTask(task_dir=self.task_dir)

    def output(self):
        """Report that the views were checked."""
        out_path = os.path.join(self.task_dir, '202_check_consumption.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        """Get the table where the view can be found."""
        return 'instance_consumption_displaced'


class CheckMlWasteViewTask(tasks_sql.SqlCheckTask):
    """Check the EOL fate propensity view.

    Check that the task-specific view for waste fate propensity prediction was created successfully.
    """
    
    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    
    def requires(self):
        """Require that the task-specific views were built."""
        return BuildMlViewsTask(task_dir=self.task_dir)

    def output(self):
        """Report that the views were checked."""
        out_path = os.path.join(self.task_dir, '203_check_waste.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        """Get the table where the view can be found."""
        return 'instance_waste_displaced'


class CheckMlTradeViewTask(tasks_sql.SqlCheckTask):
    """Check the goods and materials trade view.

    Check that the task-specific view for goods and materials trade prediction was created
    successfully.
    """
    
    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    
    def requires(self):
        """Require that the task-specific views were built."""
        return BuildMlViewsTask(task_dir=self.task_dir)

    def output(self):
        """Report that the views were checked."""
        out_path = os.path.join(self.task_dir, '204_check_trade.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        """Get the table where the view can be found."""
        return 'instance_trade_displaced'


class CheckMlWasteTradeViewTask(tasks_sql.SqlCheckTask):
    """Check that the task-specific view for waste trade prediction was created successfully."""
    
    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    
    def requires(self):
        """Require that the task-specific views were built."""
        return BuildMlViewsTask(task_dir=self.task_dir)

    def output(self):
        """Report that the views were checked."""
        out_path = os.path.join(self.task_dir, '205_check_trade.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        """Get the table where the view can be found."""
        return 'instance_waste_trade_displaced'
