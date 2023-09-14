import json
import os

import luigi

import const
import tasks_auxiliary
import tasks_preprocess


class PreprocessTask(luigi.Task):
    
    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return tasks_preprocess.CheckFrameTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '0_preprocess.json')
        return luigi.LocalTarget(out_path)

    def run(self):
        with self.input().open('r') as f:
            job_info = json.load(f)

        with self.output().open('w') as f:
            return json.dump(job_info, f)


class AuxiliaryTask(luigi.Task):
    
    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return tasks_auxiliary.CheckFrameAuxTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '1_auxiliary.json')
        return luigi.LocalTarget(out_path)

    def run(self):
        with self.input().open('r') as f:
            job_info = json.load(f)

        with self.output().open('w') as f:
            return json.dump(job_info, f)
