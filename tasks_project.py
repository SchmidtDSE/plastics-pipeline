import json
import os
import pickle

import luigi

import const
import tasks_curve
import tasks_ml
import tasks_project_template


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


class SeedConsumptionProjectionTask(tasks_project_template.SeedProjectionTask):

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    
    def requires(self):
        return PreCheckProjectTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '501_seed_consumption.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        return 'project_consumption_ml'


class CheckSeedConsumptionProjectionTask(tasks_project_template.CheckSeedProjectionTask):

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    
    def requires(self):
        return SeedConsumptionProjectionTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '502_check_seed_consumption.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        return 'project_consumption_ml'


class ProjectConsumptionRawTask(tasks_project_template.ProjectRawTask):

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    
    def requires(self):
        return CheckSeedConsumptionProjectionTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '503_project_consumption_raw.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        return 'project_consumption_ml'

    def get_consumption_model_filename(self):
        return 'consumption.pickle'

    def get_waste_model_filename(self):
        return 'waste.pickle'

    def get_trade_model_filename(self):
        return 'trade.pickle'

    def hot_encode(self, candidate, hot_value):
        return 1 if candidate == hot_value else 0

    def get_consumption_inputs_sql(self, year, region, sector):
        template_vals = {
            'table_name': self.get_table_name(),
            'year': year,
            'region': region,
            'sector': sector,
            'flagChina': self.hot_encode(region, 'china'),
            'flagEU30': self.hot_encode(region, 'eu30'),
            'flagNafta': self.hot_encode(region, 'nafta'),
            'flagRow': self.hot_encode(region, 'row'),
            'flagAgriculture': self.hot_encode(sector, 'consumptionAgricultureMT'),
            'flagConstruction': self.hot_encode(sector, 'consumptionConstructionMT'),
            'flagElectronic': self.hot_encode(sector, 'consumptionElectronicMT'),
            'flagHouseholdLeisureSports': self.hot_encode(sector, 'consumptionHouseholdLeisureSportsMT'),
            'flagOther': self.hot_encode(sector, 'consumptionOtherMT'),
            'flagPackaging': self.hot_encode(sector, 'consumptionPackagingMT'),
            'flagTextile': self.hot_encode(sector, 'consumptionTextileMT'),
            'flagTransporation': self.hot_encode(sector, 'consumptionTransporationMT'),
        }

        return '''
            SELECT
                after.year - before.year AS years,
                (after.population - before.population) / before.population AS popChange,
                (after.gdp - before.gdp) / before.gdp AS gdpChange,
                {flagChina} AS flagChina,
                {flagEU30} AS flagEU30,
                {flagNafta} AS flagNafta,
                {flagRow} AS flagRow,
                {flagAgriculture} AS flagAgriculture,
                {flagConstruction} AS flagConstruction,
                {flagElectronic} AS flagElectronic,
                {flagHouseholdLeisureSports} AS flagHouseholdLeisureSports,
                {flagOther} AS flagOther,
                {flagPackaging} AS flagPackaging,
                {flagTextile} AS flagTextile,
                {flagTransporation} AS flagTransporation,
                before.beforeValue AS beforeValue
            FROM
                (
                    SELECT
                        year AS year,
                        population AS population,
                        gdp AS gdp
                    FROM
                        {table_name}
                    WHERE
                        year = {year}
                        AND region = '{region}'
                ) after
            INNER JOIN
                (
                    SELECT
                        year AS year,
                        population AS population,
                        gdp AS gdp,
                        {sector} AS beforeValue
                    FROM
                        {table_name}
                    WHERE
                        year - {year} <= 5
                        AND year - {year} > 0
                        AND region = '{region}'
                ) before
        '''.format(**template_vals)

    def get_waste_inputs_sql(self, year, region, type_name):
        template_vals = {
            'table_name': self.get_table_name(),
            'year': year,
            'region': region,
            'typeName': type_name,
            'flagChina': self.hot_encode(region, 'china'),
            'flagEU30': self.hot_encode(region, 'eu30'),
            'flagNafta': self.hot_encode(region, 'nafta'),
            'flagRow': self.hot_encode(region, 'row'),
            'flagRecycling': self.hot_encode(type_name, 'eolRecyclingPercent'),
            'flagIncineration': self.hot_encode(type_name, 'eolIncinerationPercent'),
            'flagLandfill': self.hot_encode(type_name, 'eolLandfillPercent'),
            'flagMismanaged': self.hot_encode(type_name, 'eolMismanagedPercent')
        }

        return '''
            SELECT
                after.year - before.year AS years,
                (after.population - before.population) / before.population AS popChange,
                (after.gdp - before.gdp) / before.gdp AS gdpChange,
                after.gdp AS afterGdp,
                before.beforeValue AS beforePercent,
                {flagChina} AS flagChina,
                {flagEU30} AS flagEU30,
                {flagNafta} AS flagNafta,
                {flagRow} AS flagRow,
                {flagRecycling} AS flagRecycling,
                {flagIncineration} AS flagIncineration,
                {flagLandfill} AS flagLandfill,
                {flagMismanaged} AS flagMismanaged
            FROM
                (
                    SELECT
                        year AS year,
                        population AS population,
                        gdp AS gdp
                    FROM
                        {table_name}
                    WHERE
                        year = {year}
                        AND region = '{region}'
                ) after
            INNER JOIN
                (
                    SELECT
                        year AS year,
                        population AS population,
                        gdp AS gdp,
                        {typeName} AS beforeValue
                    FROM
                        {table_name}
                    WHERE
                        year - {year} <= 5
                        AND year - {year} > 0
                        AND region = '{region}'
                ) before
        '''.format(**template_vals)

    def get_trade_inputs_sql(self, year, region, type_name):
        template_vals = {
            'table_name': self.get_table_name(),
            'year': year,
            'region': region,
            'typeName': type_name,
            'flagChina': self.hot_encode(region, 'china'),
            'flagEU30': self.hot_encode(region, 'eu30'),
            'flagNafta': self.hot_encode(region, 'nafta'),
            'flagRow': self.hot_encode(region, 'row'),
            'flagArticles': self.hot_encode(type_name, 'netImportArticlesMT'),
            'flagFibers': self.hot_encode(type_name, 'netImportFibersMT'),
            'flagGoods': self.hot_encode(type_name, 'netImportGoodsMT'),
            'flagResin': self.hot_encode(type_name, 'netImportResinMT')
        }

        return '''
            SELECT
                after.year - before.year AS years,
                (after.population - before.population) / before.population AS popChange,
                (after.gdp - before.gdp) / before.gdp AS gdpChange,
                {flagChina} AS flagChina,
                {flagEU30} AS flagEU30,
                {flagNafta} AS flagNafta,
                {flagRow} AS flagRow,
                {flagArticles} AS flagArticles,
                {flagFibers} AS flagFibers,
                {flagGoods} AS flagGoods,
                {flagResin} AS flagResin,
                before.beforeValue AS beforeValue
            FROM
                (
                    SELECT
                        year AS year,
                        population AS population,
                        gdp AS gdp
                    FROM
                        {table_name}
                    WHERE
                        year = {year}
                        AND region = '{region}'
                ) after
            INNER JOIN
                (
                    SELECT
                        year AS year,
                        population AS population,
                        gdp AS gdp,
                        {typeName} AS beforeValue
                    FROM
                        {table_name}
                    WHERE
                        year - {year} <= 5
                        AND year - {year} > 0
                        AND region = '{region}'
                ) before
        '''.format(**template_vals)
    
    def get_consumption_inputs_cols(self):
        return [
            'years',
            'popChange',
            'gdpChange',
            'flagChina',
            'flagEU30',
            'flagNafta',
            'flagRow',
            'flagAgriculture',
            'flagConstruction',
            'flagElectronic',
            'flagHouseholdLeisureSports',
            'flagOther',
            'flagPackaging',
            'flagTextile',
            'flagTransporation',
            'beforeValue'
        ]
    
    def get_waste_inputs_cols(self):
        return [
            'years',
            'popChange',
            'gdpChange',
            'afterGdp',
            'beforePercent',
            'flagChina',
            'flagEU30',
            'flagNafta',
            'flagRow',
            'flagRecycling',
            'flagIncineration',
            'flagLandfill',
            'flagMismanaged'
        ]
    
    def get_trade_inputs_cols(self):
        return [
            'years',
            'popChange',
            'gdpChange',
            'flagChina',
            'flagEU30',
            'flagNafta',
            'flagRow',
            'flagArticles',
            'flagFibers',
            'flagGoods',
            'flagResin',
            'beforeValue'
        ]

    def transform_consumption_prediction(self, instance, prediction):
        return instance['beforeValue'] * (1 + prediction)

    def transform_waste_prediction(self, instance, prediction):
        return prediction

    def transform_trade_prediction(self, instance, prediction):
        raise instance['beforeValue'] * (1 + prediction)
