import json
import os

import luigi

import const
import tasks_auxiliary
import tasks_curve
import tasks_export
import tasks_ml
import tasks_ml_prep
import tasks_preprocess
import tasks_project


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


class MlPrepTask(luigi.Task):
    
    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return [
            tasks_ml_prep.CheckMlConsumptionViewTask(task_dir=self.task_dir),
            tasks_ml_prep.CheckMlWasteViewTask(task_dir=self.task_dir),
            tasks_ml_prep.CheckMlTradeViewTask(task_dir=self.task_dir)
        ]

    def output(self):
        out_path = os.path.join(self.task_dir, '2_ml_prep.json')
        return luigi.LocalTarget(out_path)

    def run(self):
        with self.input()[0].open('r') as f:
            job_info = json.load(f)

        with self.output().open('w') as f:
            return json.dump(job_info, f)


class MlTask(luigi.Task):
    
    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return [
            tasks_ml.CheckSweepConsumptionTask(task_dir=self.task_dir),
            tasks_ml.CheckSweepWasteTask(task_dir=self.task_dir),
            tasks_ml.CheckSweepTradeTask(task_dir=self.task_dir),
            tasks_ml.CheckSweepWasteTradeTask(task_dir=self.task_dir)
        ]

    def output(self):
        out_path = os.path.join(self.task_dir, '3_ml.json')
        return luigi.LocalTarget(out_path)

    def run(self):
        with self.input()[0].open('r') as f:
            job_info = json.load(f)

        with self.output().open('w') as f:
            return json.dump(job_info, f)


class CurveTask(luigi.Task):
    
    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return [
            tasks_curve.ConsumptionCurveTask(task_dir=task_dir),
            tasks_curve.ConsumptionCurveNaiveTask(task_dir=task_dir),
            tasks_curve.WasteCurveTask(task_dir=task_dir),
            tasks_curve.WasteCurveNaiveTask(task_dir=task_dir),
            tasks_curve.TradeCurveTask(task_dir=task_dir),
            tasks_curve.TradeCurveNaiveTask(task_dir=task_dir)
        ]

    def output(self):
        out_path = os.path.join(self.task_dir, '4_curve.json')
        return luigi.LocalTarget(out_path)

    def run(self):
        with self.input()[0].open('r') as f:
            job_info = json.load(f)

        with self.output().open('w') as f:
            return json.dump(job_info, f)


class ProjectTask(luigi.Task):
    
    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return [
            tasks_project.MlLifecycleCheckTask(task_dir=self.task_dir),
            tasks_project.CurveLifecycleCheckTask(task_dir=self.task_dir),
            tasks_project.ApplyLifecycleNaiveTask(task_dir=self.task_dir)
        ]

    def output(self):
        out_path = os.path.join(self.task_dir, '5_project.json')
        return luigi.LocalTarget(out_path)

    def run(self):
        with self.input()[0].open('r') as f:
            job_info = json.load(f)

        with self.output().open('w') as f:
            return json.dump(job_info, f)


class ExportTask(luigi.Task):

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        return [
            tasks_export.ExportMlTask(task_dir=self.task_dir),
            tasks_export.ExportCurveTask(task_dir=self.task_dir),
            tasks_export.ExportNaiveTask(task_dir=self.task_dir)
        ]

    def output(self):
        out_path = os.path.join(self.task_dir, '6_export.json')
        return luigi.LocalTarget(out_path)

    def run(self):
        with self.input()[0].open('r') as f:
            job_info = json.load(f)

        with self.output().open('w') as f:
            return json.dump(job_info, f)
