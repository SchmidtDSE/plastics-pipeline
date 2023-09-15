import csv
import json
import os
import sqlite3

import luigi

import const
import tasks_project


class ExportTemplateTask(luigi.Task):

    def run(self):
        with self.input().open('r') as f:
            job_info = json.load(f)

        database_loc = job_info['database']
        connection = sqlite3.connect(database_loc)
        cursor = connection.cursor()

        with open(os.path.join(const.SQL_DIR, '09_export', 'export.sql')) as f:
            sql_contents_template = f.read()
            sql_contents = self.transform_sql(sql_contents_template)

        records = map(
            lambda x: self.parse_record(x),
            cursor.execute(sql_contents)
        )
        
        output_path = os.path.join(
            job_info['directories']['output'],
            self.get_output_filename()
        )
        with open(output_path, 'w') as f:
            writer = csv.DictWriter(f, fieldnames=const.EXPORT_FIELD_NAMES)
            writer.writeheader()
            writer.writerows(records)

        cursor.close()
        connection.close()

        with self.output().open('w') as f:
            return json.dump(job_info, f)

    def parse_record(self, target):
        return dict(zip(const.EXPORT_FIELD_NAMES, target))

    def transform_sql(self, sql_contents):
        return sql_contents.format(table_name=self.get_table_name())

    def get_table_name(self):
        raise NotImplementedError('Must use implementor.')

    def get_output_filename(self):
        raise NotImplementedError('Must use implementor.')


class ExportMlTask(ExportTemplateTask):

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return tasks_project.MlLifecycleCheckTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '600_export_ml.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        return 'project_ml'

    def get_output_filename(self):
        return 'overview_ml.csv'


class ExportCurveTask(ExportTemplateTask):

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return tasks_project.CurveLifecycleCheckTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '601_export_curve.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        return 'project_curve'

    def get_output_filename(self):
        return 'overview_curve.csv'


class ExportNaiveTask(ExportTemplateTask):

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return tasks_project.ApplyLifecycleNaiveTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '602_export_naive.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        return 'project_naive'

    def get_output_filename(self):
        return 'overview_naive.csv'

