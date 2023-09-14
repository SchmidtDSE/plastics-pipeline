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
    
    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return tasks_workspace.CleanFilenamesTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '003_prepare_import_files.json')
        return luigi.LocalTarget(out_path)

    def run(self):
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
    
    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return PrepareImportFilesTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '004_execute_import_files.json')
        return luigi.LocalTarget(out_path)

    def run(self):
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

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    
    def requires(self):
        return ExecuteImportFilesTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '005_check_import_files.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        return 'file_01productionofresinnofiber'


class CleanInputsTask(tasks_sql.SqlExecuteTask):
    
    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    
    def requires(self):
        return CheckImportTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '006_clean_inputs.json')
        return luigi.LocalTarget(out_path)

    def get_scripts(self):
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
            '02_clean_inputs/raw_production_fiber.sql',
            '02_clean_inputs/raw_production_resin.sql'
        ]


class CheckCleanInputsTask(tasks_sql.SqlCheckTask):
    
    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    
    def requires(self):
        return CleanInputsTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '007_check_clean_inputs.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        return 'raw_additives'


class BuildViewsTask(tasks_sql.SqlExecuteTask):
    
    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    
    def requires(self):
        return CheckCleanInputsTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '008_build_views.json')
        return luigi.LocalTarget(out_path)

    def get_scripts(self):
        return [
            '03_views/consumption.sql',
            '03_views/end_use.sql',
            '03_views/eol.sql',
            '03_views/input_additives.sql',
            '03_views/input_import.sql',
            '03_views/input_production.sql',
            '03_views/inputs.sql',
            '03_views/net_imports.sql',
            '03_views/overview_consumption.sql',
            '03_views/overview_end_use.sql',
            '03_views/overview_eol.sql',
            '03_views/overview_inputs.sql',
            '03_views/overview_net_imports.sql',
            '03_views/overview_sector_trade.sql',
            '03_views/summary_percents.sql',
            '03_views/summary.sql'
        ]


class CheckViewsTask(tasks_sql.SqlCheckTask):
    
    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    
    def requires(self):
        return BuildViewsTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '009_check_views.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        return 'summary'


class BuildFrameTask(luigi.Task):

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return CheckViewsTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '010_build_frame.json')
        return luigi.LocalTarget(out_path)
    
    def run(self):
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
        connection.close()

        with self.output().open('w') as f:
            return json.dump(job_info, f)


class CheckFrameTask(luigi.Task):

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    
    def requires(self):
        return BuildFrameTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '011_check_frame.json')
        return luigi.LocalTarget(out_path)

    def run(self):
        with self.input().open('r') as f:
            job_info = json.load(f)

        preprocessed_output_path = os.path.join(
            job_info['directories']['output'],
            'preprocessed.csv'
        )

        check_summary.check(preprocessed_output_path)

        with self.output().open('w') as f:
            return json.dump(job_info, f)
