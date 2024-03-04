"""Tasks which export files from the pipeline in order to run the tool.

Tasks which export files from the pipeline including projections that are required to run the
decision support tool like at https://global-plastics-tool.org.

License:
    BSD, see LICENSE.md
"""

import csv
import json
import os
import sqlite3

import luigi

import const
import sql_util
import tasks_project


class ExportTemplateTask(luigi.Task):
    """Template Method which defines a Luigi Task to export projections."""

    def run(self):
        """Export a set of projections to a CSV file.

        Export a set of projections to a CSV file from a single type of model (ML, curve, naive) as
        required for the decision support tool.
        """
        with self.input().open('r') as f:
            job_info = json.load(f)

        database_loc = job_info['database']
        connection = sqlite3.connect(database_loc)
        cursor = connection.cursor()

        additional_vals = self.get_additional_template_vals()
        sql_contents = sql_util.get_sql_file(
            'export.sql',
            sql_dir='10_export',
            additional_params=additional_vals
        )

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
        """Parse a single output record from the SQLite database.

        Args:
            target: Vector returned from the database to be converted to a dict describing a single
                projection.

        Returns:
            Projection instance parsed from a raw vector returned by the database.
        """
        return dict(zip(const.EXPORT_FIELD_NAMES, target))

    def get_additional_template_vals(self):
        """Provide additional template values for jinja.

        Returns:
            Mapping from name to value or None if no additional values.
        """
        return {'table_name': self.get_table_name()}

    def get_table_name(self):
        """Get the name of the table to be queried for model projections.

        Returns:
            The name of the table to be queried for the model's output.
        """
        raise NotImplementedError('Must use implementor.')

    def get_output_filename(self):
        """Get the path at which the output / export should be written.

        Returns:
            The path at which the export should be written.
        """
        raise NotImplementedError('Must use implementor.')


class ExportMlTask(ExportTemplateTask):
    """Tasks to export machine learning projections."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        """Require that lifecycle estimation on ML results be done prior to export."""
        return tasks_project.MlLifecycleCheckTask(task_dir=self.task_dir)

    def output(self):
        """Report that machine learning predictions have been exported."""
        out_path = os.path.join(self.task_dir, '600_export_ml.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        """Get the table at which the the machine learning predictions can be queried."""
        return 'project_ml'

    def get_output_filename(self):
        """Get the filename at which the export should be written."""
        return 'overview_ml.csv'


class ExportCurveTask(ExportTemplateTask):
    """Tasks to export curve projections."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        """Require that lifecycle estimation on curve results be done prior to export."""
        return tasks_project.CurveLifecycleCheckTask(task_dir=self.task_dir)

    def output(self):
        """Report that curve predictions have been exported."""
        out_path = os.path.join(self.task_dir, '601_export_curve.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        """Get the table at which the the curve predictions can be queried."""
        return 'project_curve'

    def get_output_filename(self):
        """Get the filename at which the export should be written."""
        return 'overview_curve.csv'


class ExportNaiveTask(ExportTemplateTask):
    """Tasks to export naive projections."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        """Require that lifecycle estimation on naive results be done prior to export."""
        return tasks_project.ApplyLifecycleNaiveTask(task_dir=self.task_dir)

    def output(self):
        """Report that naive predictions have been exported."""
        out_path = os.path.join(self.task_dir, '602_export_naive.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        """Get the table at which the the naive predictions can be queried."""
        return 'project_naive'

    def get_output_filename(self):
        """Get the filename at which the export should be written."""
        return 'overview_naive.csv'
