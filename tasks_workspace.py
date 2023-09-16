import json
import os
import random
import shutil

import luigi

import clean_filenames
import const


class PrepareWorkspaceTask(luigi.Task):

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def output(self):
        out_path = os.path.join(self.task_dir, '001_task_start.json')
        return luigi.LocalTarget(out_path)

    def run(self):
        with open(os.path.join(self.task_dir, 'job.json')) as f:
            job_info = json.load(f)

        random.seed(1234)

        if not os.path.exists(job_info['directories']['output']):
            os.makedirs(job_info['directories']['output'])

        shutil.copytree(
            job_info['directories']['data'],
            job_info['directories']['workspace'],
            dirs_exist_ok=True
        )

        with self.output().open('w') as f:
            json.dump(job_info, f)


class CleanFilenamesTask(luigi.Task):

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return PrepareWorkspaceTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '002_task_clean_files.json')
        return luigi.LocalTarget(out_path)

    def run(self):
        with self.input().open('r') as f:
            job_info = json.load(f)

        clean_filenames.execute(job_info['directories']['workspace'])

        sample_file = os.path.join(
            job_info['directories']['workspace'],
            '01productionofresinnofiber.csv'
        )
        
        with open(sample_file) as f:
            sample_contents = f.read()

        assert sample_contents != ''

        with self.output().open('w') as f:
            json.dump(job_info, f)
