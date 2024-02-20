"""Top-level tasks that can be used to execute major components of the pipeline.

Top-level tasks that can be used to execute major components of the plastics machine learning
training pipeline.

License:
    BSD, see LICENSE.md
"""

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
    """Task which requires that data are preprocessed and loaded into the scratch SQLite db."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        """Require that the preprocessed data are loaded and tested."""
        return tasks_preprocess.CheckFrameTask(task_dir=self.task_dir)

    def output(self):
        """Report that the stage 0 tasks are complete."""
        out_path = os.path.join(self.task_dir, '0_preprocess.json')
        return luigi.LocalTarget(out_path)

    def run(self):
        """Report preprocessed data are loaded into the scratch SQLite database."""
        with self.input().open('r') as f:
            job_info = json.load(f)

        with self.output().open('w') as f:
            return json.dump(job_info, f)


class AuxiliaryTask(luigi.Task):
    """Task which requires non-plastics data used by the modeling are loaded into the SQLite db."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        """Require that the auxiliary data required by modeling are loaded and tested."""
        return tasks_auxiliary.CheckFrameAuxTask(task_dir=self.task_dir)

    def output(self):
        """Report that stage 1 tasks are complete."""
        out_path = os.path.join(self.task_dir, '1_auxiliary.json')
        return luigi.LocalTarget(out_path)

    def run(self):
        """Report preprocessed auxiliary data are loaded into the scratch SQLite database."""
        with self.input().open('r') as f:
            job_info = json.load(f)

        with self.output().open('w') as f:
            return json.dump(job_info, f)


class MlPrepTask(luigi.Task):
    """Task requiring the machine learning instances are generated ahead of training."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        """Require that the SQLite db tables for the machine learning tasks are built and tested."""
        return [
            tasks_ml_prep.CheckMlConsumptionViewTask(task_dir=self.task_dir),
            tasks_ml_prep.CheckMlWasteViewTask(task_dir=self.task_dir),
            tasks_ml_prep.CheckMlTradeViewTask(task_dir=self.task_dir)
        ]

    def output(self):
        """Report that the stage 2 tasks are done."""
        out_path = os.path.join(self.task_dir, '2_ml_prep.json')
        return luigi.LocalTarget(out_path)

    def run(self):
        """Report that the ML data are built."""
        with self.input()[0].open('r') as f:
            job_info = json.load(f)

        with self.output().open('w') as f:
            return json.dump(job_info, f)


class MlTask(luigi.Task):
    """Task which requires the machine learning sweeps for trade, consumption, and waste."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        """Require execution and check of the machine learning sweeps."""
        return [
            tasks_ml.CheckSweepConsumptionTask(task_dir=self.task_dir),
            tasks_ml.CheckSweepWasteTask(task_dir=self.task_dir),
            tasks_ml.CheckSweepTradeTask(task_dir=self.task_dir),
            tasks_ml.CheckSweepWasteTradeTask(task_dir=self.task_dir)
        ]

    def output(self):
        """Report that stage 3 tasks are done."""
        out_path = os.path.join(self.task_dir, '3_ml.json')
        return luigi.LocalTarget(out_path)

    def run(self):
        """Report that the machine learning tasks are done."""
        with self.input()[0].open('r') as f:
            job_info = json.load(f)

        with self.output().open('w') as f:
            return json.dump(job_info, f)


class CurveTask(luigi.Task):
    """Task requiring that the curve modeling tasks are completed."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        """Require that the curve and naive learning branches are executed."""
        return [
            tasks_curve.ConsumptionCurveTask(task_dir=CurveTask.task_dir),
            tasks_curve.ConsumptionCurveNaiveTask(task_dir=CurveTask.task_dir),
            tasks_curve.WasteCurveTask(task_dir=CurveTask.task_dir),
            tasks_curve.WasteCurveNaiveTask(task_dir=CurveTask.task_dir),
            tasks_curve.TradeCurveTask(task_dir=CurveTask.task_dir),
            tasks_curve.TradeCurveNaiveTask(task_dir=CurveTask.task_dir)
        ]

    def output(self):
        """Report that stage 4 tasks are done."""
        out_path = os.path.join(self.task_dir, '4_curve.json')
        return luigi.LocalTarget(out_path)

    def run(self):
        """Report that the curve and naive branches are complete."""
        with self.input()[0].open('r') as f:
            job_info = json.load(f)

        with self.output().open('w') as f:
            return json.dump(job_info, f)


class ProjectTask(luigi.Task):
    """Task which requires generation of projection dataset.

    Task requiring generation of datasets for future plastics projections using ML, curve, and naive
    models.
    """

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        """Require that the ML, curve, and naive projections are written to the SQLite db."""
        return [
            tasks_project.MlApplyWasteTradeTask(task_dir=self.task_dir),
            tasks_project.CurveApplyWasteTradeTask(task_dir=self.task_dir),
            tasks_project.NaiveApplyWasteTradeTask(task_dir=self.task_dir)
        ]

    def output(self):
        """Report that stage 5 tasks are complete."""
        out_path = os.path.join(self.task_dir, '5_project.json')
        return luigi.LocalTarget(out_path)

    def run(self):
        """Report that the projection datasets are complete using ML, curve, and naive models."""
        with self.input()[0].open('r') as f:
            job_info = json.load(f)

        with self.output().open('w') as f:
            return json.dump(job_info, f)


class ExportTask(luigi.Task):
    """Task requiring the export of a CSV file usable by the downstream tool."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        """Require that the ML, curve, and naive projections are written to CSV files."""
        return [
            tasks_export.ExportMlTask(task_dir=self.task_dir),
            tasks_export.ExportCurveTask(task_dir=self.task_dir),
            tasks_export.ExportNaiveTask(task_dir=self.task_dir)
        ]

    def output(self):
        """Report that stage 6 tasks are complete."""
        out_path = os.path.join(self.task_dir, '6_export.json')
        return luigi.LocalTarget(out_path)

    def run(self):
        """Report that the CSV files required for downstream usage have been written."""
        with self.input()[0].open('r') as f:
            job_info = json.load(f)

        with self.output().open('w') as f:
            return json.dump(job_info, f)
