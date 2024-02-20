"""Simple unit tests to validate that tasks initialize.

Most of the assertions and testing happen in tasks themselves including separate check tasks. That
said, this unit test simply validates that those tasks initialize.

@license BSD
"""

import unittest

import tasks
import tasks_auxiliary
import tasks_curve
import tasks_export
import tasks_ml_prep
import tasks_ml
import tasks_preprocess
import tasks_project
import tasks_workspace


class TaskTests(unittest.TestCase):

    def check_task_init(self, task):
        self.assertIsNotNone(task)
        self.assertIsNotNone(task.requires())

    def test_tasks_auxiliary(self):
        self.check_task_init(tasks_auxiliary.ProcessGdpTask())
        self.check_task_init(tasks_auxiliary.ProcessRawPopulationTask())
        self.check_task_init(tasks_auxiliary.PrepareImportFilesAuxTask())
        self.check_task_init(tasks_auxiliary.ExecuteImportFilesAuxTask())
        self.check_task_init(tasks_auxiliary.CheckImportAuxTask())
        self.check_task_init(tasks_auxiliary.BuildViewsAuxTask())
        self.check_task_init(tasks_auxiliary.CheckViewsAuxTask())
        self.check_task_init(tasks_auxiliary.BuildFrameAuxTask())

    def test_tasks_curve(self):
        self.check_task_init(tasks_curve.CurveTask())
        self.check_task_init(tasks_curve.ConsumptionCurveTask())
        self.check_task_init(tasks_curve.ConsumptionCurveNaiveTask())
        self.check_task_init(tasks_curve.WasteCurveTask())
        self.check_task_init(tasks_curve.WasteCurveNaiveTask())
        self.check_task_init(tasks_curve.TradeCurveTask())
        self.check_task_init(tasks_curve.TradeCurveNaiveTask())
        self.check_task_init(tasks_curve.WasteTradeCurveTask())
        self.check_task_init(tasks_curve.WasteTradeCurveNaiveTask())

    def test_tasks_export(self):
        self.check_task_init(tasks_export.ExportTemplateTask())
        self.check_task_init(tasks_export.ExportMlTask())
        self.check_task_init(tasks_export.ExportCurveTask())
        self.check_task_init(tasks_export.ExportNaiveTask())

    def test_tasks_ml_prep(self):
        self.check_task_init(tasks_ml_prep.CheckMlPrepTask())
        self.check_task_init(tasks_ml_prep.BuildMlViewsTask())
        self.check_task_init(tasks_ml_prep.CheckMlConsumptionViewTask())
        self.check_task_init(tasks_ml_prep.CheckMlWasteViewTask())
        self.check_task_init(tasks_ml_prep.CheckMlTradeViewTask())
        self.check_task_init(tasks_ml_prep.CheckMlWasteTradeViewTask())

    def test_tasks_ml(self):
        self.check_task_init(tasks_ml.SweepTask())
        self.check_task_init(tasks_ml.CheckSweepTask())
        self.check_task_init(tasks_ml.SweepConsumptionTask())
        self.check_task_init(tasks_ml.SweepWasteTask())
        self.check_task_init(tasks_ml.SweepTradeTask())
        self.check_task_init(tasks_ml.SweepWasteTradeTask())
        self.check_task_init(tasks_ml.CheckSweepConsumptionTask())
        self.check_task_init(tasks_ml.CheckSweepWasteTask())
        self.check_task_init(tasks_ml.CheckSweepTradeTask())
        self.check_task_init(tasks_ml.CheckSweepWasteTradeTask())

    def test_tasks_preprocess(self):
        self.check_task_init(tasks_preprocess.PrepareImportFilesTask())
        self.check_task_init(tasks_preprocess.ExecuteImportFilesTask())
        self.check_task_init(tasks_preprocess.CheckImportTask())
        self.check_task_init(tasks_preprocess.CleanInputsTask())
        self.check_task_init(tasks_preprocess.CheckCleanInputsTask())
        self.check_task_init(tasks_preprocess.BuildViewsTask())
        self.check_task_init(tasks_preprocess.CheckViewsTask())
        self.check_task_init(tasks_preprocess.BuildFrameTask())
        self.check_task_init(tasks_preprocess.CheckFrameTask())

    def test_tasks_project(self):
        self.check_task_init(tasks_project.PreCheckMlProjectTask())
        self.check_task_init(tasks_project.PreCheckCurveProjectTask())
        self.check_task_init(tasks_project.PreCheckNaiveProjectTask())
        self.check_task_init(tasks_project.SeedMlProjectionTask())
        self.check_task_init(tasks_project.CheckSeedMlProjectionTask())
        self.check_task_init(tasks_project.ProjectMlRawTask())
        self.check_task_init(tasks_project.SeedCurveProjectionTask())
        self.check_task_init(tasks_project.CheckSeedCurveProjectionTask())
        self.check_task_init(tasks_project.ProjectCurveRawTask())
        self.check_task_init(tasks_project.SeedNaiveProjectionTask())
        self.check_task_init(tasks_project.CheckSeedNaiveProjectionTask())
        self.check_task_init(tasks_project.ProjectNaiveRawTask())
        self.check_task_init(tasks_project.NormalizeMlTask())
        self.check_task_init(tasks_project.NormalizeCurveTask())
        self.check_task_init(tasks_project.NormalizeNaiveTask())
        self.check_task_init(tasks_project.CheckNormalizeMlTask())
        self.check_task_init(tasks_project.CheckNormalizeCurveTask())
        self.check_task_init(tasks_project.ApplyLifecycleMLTask())
        self.check_task_init(tasks_project.ApplyLifecycleCurveTask())
        self.check_task_init(tasks_project.ApplyLifecycleNaiveTask())
        self.check_task_init(tasks_project.MlLifecycleCheckTask())
        self.check_task_init(tasks_project.CurveLifecycleCheckTask())
        self.check_task_init(tasks_project.MlApplyWasteTradeTask())
        self.check_task_init(tasks_project.CurveApplyWasteTradeTask())
        self.check_task_init(tasks_project.NaiveApplyWasteTradeTask())

    def test_tasks_workspace(self):
        self.check_task_init(tasks_workspace.PrepareWorkspaceTask())
        self.check_task_init(tasks_workspace.CleanFilenamesTask())

    def test_tasks_top_level(self):
        self.check_task_init(tasks.PreprocessTask())
        self.check_task_init(tasks.AuxiliaryTask())
        self.check_task_init(tasks.MlPrepTask())
        self.check_task_init(tasks.MlTask())
        self.check_task_init(tasks.CurveTask())
        self.check_task_init(tasks.ProjectTask())
        self.check_task_init(tasks.ExportTask())
