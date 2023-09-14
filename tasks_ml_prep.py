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


class CheckPrepTask(luigi.Task):

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return {
            'auxiliary': tasks_auxiliary.CheckViewsAuxTask(task_dir=self.task_dir),
            'preprocess': tasks_preprocess.CheckViewsTask(task_dir=self.task_dir)
        }

    def output(self):
        out_path = os.path.join(self.task_dir, '200_check_prep.json')
        return luigi.LocalTarget(out_path)

    def run(self):
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

        connection.close()

        with self.output().open('w') as f:
            return json.dump(job_info, f)



class BuildViewsTask(tasks_sql.SqlExecuteTask):
    
    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    
    def requires(self):
        return 

    def output(self):
        out_path = os.path.join(self.task_dir, '201_build_views.json')
        return luigi.LocalTarget(out_path)

    def get_scripts(self):
        return [
            '07_aux/yearly_instances.sql',
            '07_aux/displaced_instances.sql'
        ]

