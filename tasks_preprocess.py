"""Tasks to pre-process data prior to any task-specific modeling.

Tasks to pre-process data prior to any task-specific modeling. Unlike tasks_ml, these preprocessing
steps are not specific to any individual modeling task.

License:
    BSD, see LICENSE.md
"""

import csv
import datetime
import json
import os
import sqlite3
import subprocess

import luigi

import check_summary
import const
import tasks_sql
import tasks_workspace


class PrepareImportFilesTask(luigi.Task):
    """Task which prepares a script to import raw data files."""
    
    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        """Require that filenames have been cleaned."""
        return tasks_workspace.CleanFilenamesTask(task_dir=self.task_dir)

    def output(self):
        """Indicate that the script for importing files has been rendered."""
        out_path = os.path.join(self.task_dir, '003_prepare_import_files.json')
        return luigi.LocalTarget(out_path)

    def run(self):
        """Render the script for importing raw data files."""
        with self.input().open('r') as f:
            job_info = json.load(f)

        template_path = os.path.join(
            const.SQL_DIR,
            '01_import_files',
            'import_files.sql'
        )
        
        with open(template_path) as f:
            contents = f.read()

        target_dir = job_info['directories']['workspace'] + os.path.sep

        rendered = contents.format(target_dir=target_dir)

        output_path = os.path.join(
            job_info['directories']['workspace'],
            'import_files_rendered.sql'
        )

        with open(output_path, 'w') as f:
            f.write(rendered)

        with self.output().open('w') as f:
            json.dump(job_info, f)


class ExecuteImportFilesTask(luigi.Task):
    """Execute the import of raw data files."""
    
    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        """Require that the raw data import script have been rendered."""
        return PrepareImportFilesTask(task_dir=self.task_dir)

    def output(self):
        """Indicate that files have been imported."""
        out_path = os.path.join(self.task_dir, '004_execute_import_files.json')
        return luigi.LocalTarget(out_path)

    def run(self):
        """Execute the file import script."""
        with self.input().open('r') as f:
            job_info = json.load(f)

        sql_path = os.path.join(
            job_info['directories']['workspace'],
            'import_files_rendered.sql'
        )

        db_path = job_info['database']

        command = 'cat {sql_path} | sqlite3 {db_path}'.format(
            sql_path = sql_path,
            db_path = db_path
        )

        subprocess.run(command, shell=True)

        with self.output().open('w') as f:
            json.dump(job_info, f)


class CheckImportTask(tasks_sql.SqlCheckTask):
    """Task which checks that raw data files were imported correctly."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    
    def requires(self):
        """Require that raw files have been imported."""
        return ExecuteImportFilesTask(task_dir=self.task_dir)

    def output(self):
        """Report that raw data files have been imported."""
        out_path = os.path.join(self.task_dir, '005_check_import_files.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        """Provide the table name to check has contents."""
        return 'file_01productionofresinnofiber'


class CleanInputsTask(tasks_sql.SqlExecuteTask):
    """Task which performs data transformations and input data cleaning."""
    
    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    
    def requires(self):
        """Require that raw data files have been imported."""
        return CheckImportTask(task_dir=self.task_dir)

    def output(self):
        """Report that data cleaning views have been established."""
        out_path = os.path.join(self.task_dir, '006_clean_inputs.json')
        return luigi.LocalTarget(out_path)

    def get_scripts(self):
        """Return list of scripts to clean raw data inputs."""
        return [
            '02_clean_inputs/raw_additives.sql',
            '02_clean_inputs/raw_end_use_china.sql',
            '02_clean_inputs/raw_end_use_eu30.sql',
            '02_clean_inputs/raw_end_use_nafta.sql',
            '02_clean_inputs/raw_end_use_row.sql',
            '02_clean_inputs/raw_eol_china.sql',
            '02_clean_inputs/raw_eol_eu30.sql',
            '02_clean_inputs/raw_eol_nafta.sql',
            '02_clean_inputs/raw_eol_row.sql',
            '02_clean_inputs/raw_net_import_articles.sql',
            '02_clean_inputs/raw_net_import_fibers.sql',
            '02_clean_inputs/raw_net_import_finished_goods.sql',
            '02_clean_inputs/raw_net_import_resin.sql',
            '02_clean_inputs/raw_net_trade_china.sql',
            '02_clean_inputs/raw_net_trade_eu30.sql',
            '02_clean_inputs/raw_net_trade_nafta.sql',
            '02_clean_inputs/raw_net_trade_row.sql',
            '02_clean_inputs/raw_waste_trade.sql',
            '02_clean_inputs/raw_production_fiber.sql',
            '02_clean_inputs/raw_production_resin.sql',
            '02_clean_inputs/raw_future.sql'
        ]


class CheckCleanInputsTask(tasks_sql.SqlCheckTask):
    """Check that inputs have been cleaned successfully."""
    
    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    
    def requires(self):
        """Require that the data cleaning views have been established."""
        return CleanInputsTask(task_dir=self.task_dir)

    def output(self):
        """Report that the data cleaning views have been checked."""
        out_path = os.path.join(self.task_dir, '007_check_clean_inputs.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        """Indicate which view should be an examplar to check."""
        return 'raw_additives'


class BuildViewsTask(tasks_sql.SqlExecuteTask):
    """Build the data access convienence views used by downstream tasks."""
    
    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    
    def requires(self):
        """Require that the data cleaning views have been checked."""
        return CheckCleanInputsTask(task_dir=self.task_dir)

    def output(self):
        """Report that the convienece views have been established."""
        out_path = os.path.join(self.task_dir, '008_build_views.json')
        return luigi.LocalTarget(out_path)

    def get_scripts(self):
        """Return a list of scripts required to build the convienence views."""
        return [
            '03_views/consumption.sql',
            '03_views/end_use.sql',
            '03_views/eol.sql',
            '03_views/input_additives.sql',
            '03_views/input_import.sql',
            '03_views/input_production.sql',
            '03_views/inputs.sql',
            '03_views/net_imports.sql',
            '03_views/waste_trade.sql',
            '03_views/overview_consumption.sql',
            '03_views/overview_end_use.sql',
            '03_views/overview_eol.sql',
            '03_views/overview_inputs.sql',
            '03_views/overview_net_imports.sql',
            '03_views/overview_sector_trade.sql',
            '03_views/summary.sql'
        ]


class CheckViewsTask(tasks_sql.SqlCheckTask):
    """Check that the convienence views have been established."""
    
    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    
    def requires(self):
        """Require that the views have been built."""
        return BuildViewsTask(task_dir=self.task_dir)

    def output(self):
        """Report that the convienence views have been checked."""
        out_path = os.path.join(self.task_dir, '009_check_views.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        """Indicate which view should be an examplar to check."""
        return 'summary'


class BuildFrameTask(luigi.Task):
    """Export the main data frame used by the rest of the pipeline."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        """Require that the convienence views have been established."""
        return CheckViewsTask(task_dir=self.task_dir)

    def output(self):
        """Report that the main data frame has been built."""
        out_path = os.path.join(self.task_dir, '010_build_frame.json')
        return luigi.LocalTarget(out_path)
    
    def run(self):
        """Build the main data frame."""
        with self.input().open('r') as f:
            job_info = json.load(f)

        database_loc = job_info['database']
        connection = sqlite3.connect(database_loc)
        cursor = connection.cursor()

        sql_filename = os.path.join(
            const.SQL_DIR,
            '04_frame',
            'export.sql'
        )

        with open(sql_filename) as f:
            sql_contents = f.read()

        preprocessed_output_path = os.path.join(
            job_info['directories']['output'],
            'preprocessed.csv'
        )
        
        with open(preprocessed_output_path, 'w') as f:
            writer = csv.DictWriter(f, fieldnames=const.PREPROC_FIELD_NAMES)
            writer.writeheader()

            for row in cursor.execute(sql_contents):
                row_keyed = dict(zip(const.PREPROC_FIELD_NAMES, row))
                writer.writerow(row_keyed)

        connection.commit()

        cursor.close()
        connection.close()

        with self.output().open('w') as f:
            return json.dump(job_info, f)


class CheckFrameTask(luigi.Task):
    """Confirm that the main data frame has been built."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    
    def requires(self):
        """Require that the main data frame be built."""
        return BuildFrameTask(task_dir=self.task_dir)

    def output(self):
        """Report that the main data frame has been checked."""
        out_path = os.path.join(self.task_dir, '011_check_frame.json')
        return luigi.LocalTarget(out_path)

    def run(self):
        """Confirm that the main data frame has "valid" contents in it."""
        with self.input().open('r') as f:
            job_info = json.load(f)

        preprocessed_output_path = os.path.join(
            job_info['directories']['output'],
            'preprocessed.csv'
        )

        check_summary.check(preprocessed_output_path)

        with self.output().open('w') as f:
            return json.dump(job_info, f)
