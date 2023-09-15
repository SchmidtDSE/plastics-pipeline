import json
import os
import pickle

import luigi

import const
import tasks_curve
import tasks_ml


class PreCheckProjectTask(luigi.Task):
    
    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    
    def requires(self):
        return [
            tasks_curve.ConsumptionCurveTask(task_dir=self.task_dir),
            tasks_curve.ConsumptionCurveNaiveTask(task_dir=self.task_dir),
            tasks_curve.WasteCurveTask(task_dir=self.task_dir),
            tasks_curve.WasteCurveNaiveTask(task_dir=self.task_dir),
            tasks_curve.TradeCurveTask(task_dir=self.task_dir),
            tasks_curve.TradeCurveNaiveTask(task_dir=self.task_dir),
            tasks_ml.CheckSweepConsumptionTask(task_dir=self.task_dir),
            tasks_ml.CheckSweepWasteTask(task_dir=self.task_dir),
            tasks_ml.CheckSweepTradeTask(task_dir=self.task_dir)
        ]

    def run(self):
        with self.input()[0].open('r') as f:
            job_info = json.load(f)

        models_to_check = [
            'consumption_curve',
            'consumption_curve_naive',
            'waste_curve',
            'waste_curve_naive',
            'trade_curve',
            'trade_curve_naive',
            'consumption',
            'waste',
            'trade'
        ]

        def get_model_filename(model_name):
            return os.path.join(
                job_info['directories']['workspace'],
                model_name + '.pickle'
            )

        filenames_to_check = map(get_model_filename, models_to_check)

        for filename in filenames_to_check:
            with open(filename, 'rb') as f:
                target = pickle.load(f)
                assert 'model' in target

        with self.output().open('w') as f:
            return json.dump(job_info, f)

    def output(self):
        out_path = os.path.join(self.task_dir, '500_pre_check.json')
        return luigi.LocalTarget(out_path)
