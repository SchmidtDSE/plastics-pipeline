"""Implementors of Template Methods in tasks_project_template.

Implementors of Template Methods in tasks_project_template which use a set of related models to
project forward waste, trade, and consumption. This will, for example, use all of the machine
learning models to make future projections.

License:
    BSD, see LICENSE.md
"""

import os

import luigi

import const
import tasks_curve
import tasks_ml
import tasks_project_template


class PreCheckMlProjectTask(tasks_project_template.PreCheckProjectTask):
    """Check that machine learning models are ready for projection."""
    
    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    
    def requires(self):
        return [
            tasks_ml.CheckSweepConsumptionTask(task_dir=self.task_dir),
            tasks_ml.CheckSweepWasteTask(task_dir=self.task_dir),
            tasks_ml.CheckSweepTradeTask(task_dir=self.task_dir),
            tasks_ml.CheckSweepWasteTradeTask(task_dir=self.task_dir),
        ]

    def output(self):
        out_path = os.path.join(self.task_dir, '500_pre_check_ml.json')
        return luigi.LocalTarget(out_path)

    def get_models_to_check(self):
        return [
            'consumption',
            'waste',
            'trade',
            'wasteTrade'
        ]


class PreCheckCurveProjectTask(tasks_project_template.PreCheckProjectTask):
    """Check that "curve" models are ready for projection."""
    
    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    
    def requires(self):
        return [
            tasks_curve.ConsumptionCurveTask(task_dir=self.task_dir),
            tasks_curve.WasteCurveTask(task_dir=self.task_dir),
            tasks_curve.TradeCurveTask(task_dir=self.task_dir),
            tasks_curve.WasteTradeCurveTask(task_dir=self.task_dir)
        ]

    def output(self):
        out_path = os.path.join(self.task_dir, '501_pre_check_curve.json')
        return luigi.LocalTarget(out_path)

    def get_models_to_check(self):
        return [
            'consumption_curve',
            'waste_curve',
            'trade_curve',
            'wasteTrade_curve'
        ]


class PreCheckNaiveProjectTask(tasks_project_template.PreCheckProjectTask):
    """Check that "naive" extrapolation models are ready for projection."""
    
    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    
    def requires(self):
        return [
            tasks_curve.ConsumptionCurveNaiveTask(task_dir=self.task_dir),
            tasks_curve.WasteCurveNaiveTask(task_dir=self.task_dir),
            tasks_curve.TradeCurveNaiveTask(task_dir=self.task_dir),
            tasks_curve.WasteTradeCurveNaiveTask(task_dir=self.task_dir)
        ]

    def output(self):
        out_path = os.path.join(self.task_dir, '502_pre_check_naive.json')
        return luigi.LocalTarget(out_path)

    def get_models_to_check(self):
        return [
            'consumption_curve_naive',
            'waste_curve_naive',
            'trade_curve_naive',
            'wasteTrade_curve_naive'
        ]


class SeedMlProjectionTask(tasks_project_template.SeedProjectionTask):
    """Prepare the projections table for machine learning projections."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    
    def requires(self):
        return PreCheckMlProjectTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '501_seed_ml.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        return 'project_ml'


class CheckSeedMlProjectionTask(tasks_project_template.CheckSeedProjectionTask):
    """Confirm the projections table is built for machine learning projections."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    
    def requires(self):
        return SeedMlProjectionTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '502_check_seed_ml.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        return 'project_ml'


class ProjectMlRawTask(tasks_project_template.ProjectRawTask):
    """Perform projections using the set of related machine learning models."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    
    def requires(self):
        return CheckSeedMlProjectionTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '503_project_ml_raw.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        return 'project_ml'

    def get_consumption_model_filename(self):
        return 'consumption.pickle'

    def get_waste_model_filename(self):
        return 'waste.pickle'

    def get_trade_model_filename(self):
        return 'trade.pickle'

    def get_waste_trade_model_filename(self):
        return 'wasteTrade.pickle'

    def hot_encode(self, candidate, hot_value):
        return 1 if candidate == hot_value else 0

    def get_year_selector(self, year):
        if year > 2020:
            selector = '{year} - year <= 5 AND {year} - year > 0'
        else:
            selector = 'year - {year} <= 5 AND year - {year} > 0'

        return selector.format(year=year)

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
            'yearSelector': self.get_year_selector(year)
        }

        return '''
            SELECT
                after.year - before.year AS years,
                (after.population - before.population) / before.population AS popChange,
                (
                    after.gdp / after.population - before.gdp / before.population
                ) / (
                    before.gdp / before.population
                ) AS gdpChange,
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
                        {yearSelector}
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
            'flagMismanaged': self.hot_encode(type_name, 'eolMismanagedPercent'),
            'yearSelector': self.get_year_selector(year)
        }

        return '''
            SELECT
                after.year - before.year AS years,
                (after.population - before.population) / before.population AS popChange,
                (
                    after.gdp / after.population - before.gdp / before.population
                ) / (
                    before.gdp / before.population
                ) AS gdpChange,
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
                        {yearSelector}
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
            'flagResin': self.hot_encode(type_name, 'netImportResinMT'),
            'yearSelector': self.get_year_selector(year)
        }

        return '''
            SELECT
                after.year - before.year AS years,
                (after.population - before.population) / before.population AS popChange,
                (
                    after.gdp / after.population - before.gdp / before.population
                ) / (
                    before.gdp / before.population
                ) AS gdpChange,
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
                        {yearSelector}
                        AND region = '{region}'
                ) before
        '''.format(**template_vals)

    def get_waste_trade_inputs_sql(self, year, region, type_name):
        template_vals = {
            'table_name': self.get_table_name(),
            'year': year,
            'region': region,
            'typeName': type_name,
            'flagChina': self.hot_encode(region, 'china'),
            'flagEU30': self.hot_encode(region, 'eu30'),
            'flagNafta': self.hot_encode(region, 'nafta'),
            'flagRow': self.hot_encode(region, 'row'),
            'flagSword': 1 if year >= 2017 else 0,
            'yearSelector': self.get_year_selector(year)
        }

        return '''
            SELECT
                after.year - before.year AS years,
                (after.population - before.population) / before.population AS popChange,
                (
                    after.gdp / after.population - before.gdp / before.population
                ) / (
                    before.gdp / before.population
                ) AS gdpChange,
                {flagChina} AS flagChina,
                {flagEU30} AS flagEU30,
                {flagNafta} AS flagNafta,
                {flagRow} AS flagRow,
                {flagSword} AS flagSword,
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
                        {yearSelector}
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
            'beforePercent'
        ]

    def get_waste_trade_inputs_cols(self):
        return [
            'years',
            'popChange',
            'gdpChange',
            'flagChina',
            'flagEU30',
            'flagNafta',
            'flagRow',
            'flagSword',
            'beforePercent'
        ]

    def transform_consumption_prediction(self, instance, prediction):
        return instance['beforeValue'] * (1 + prediction)

    def get_trade_attrs(self):
        return [
            'netImportArticlesPercent',
            'netImportFibersPercent',
            'netImportGoodsPercent',
            'netImportResinPercent'
        ]

    def get_waste_trade_attrs(self):
        return [
            'netWasteTradePercent'
        ]

    def postprocess_row(self, target):
        total_consumption = sum(map(
            lambda x: target[x],
            [
                'consumptionAgricultureMT',
                'consumptionConstructionMT',
                'consumptionElectronicMT',
                'consumptionHouseholdLeisureSportsMT',
                'consumptionOtherMT',
                'consumptionPackagingMT',
                'consumptionTextileMT',
                'consumptionTransporationMT'
            ]
        ))
        target.update({
            'netImportArticlesMT': target['netImportArticlesPercent'] * total_consumption,
            'netImportFibersMT': target['netImportFibersPercent'] * total_consumption,
            'netImportGoodsMT': target['netImportGoodsPercent'] * total_consumption,
            'netImportResinMT': target['netImportResinPercent'] * total_consumption,
            'netWasteTradeMT': target['netWasteTradePercent'] * total_consumption
        })
        return target


class SeedCurveProjectionTask(tasks_project_template.SeedProjectionTask):
    """Prepare the projections table for "curve" model projections."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    
    def requires(self):
        return PreCheckCurveProjectTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '504_seed_curve.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        return 'project_curve'


class CheckSeedCurveProjectionTask(tasks_project_template.CheckSeedProjectionTask):
    """Confirm the projections table is built for "curve" model projections."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    
    def requires(self):
        return SeedCurveProjectionTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '505_check_seed_curve.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        return 'project_curve'


class ProjectCurveRawTask(tasks_project_template.ProjectRawTask):
    """Perform projections using the set of related "curve" models."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def get_sector_label(self, col):
        return {
            'consumptionAgricultureMT': 'Agriculture',
            'consumptionConstructionMT': 'Building_Construction',
            'consumptionElectronicMT': 'Electrical_Electronic',
            'consumptionHouseholdLeisureSportsMT': 'Household_Leisure_Sports',
            'consumptionOtherMT': 'Others',
            'consumptionPackagingMT': 'Packaging',
            'consumptionTextileMT': 'Textile',
            'consumptionTransporationMT': 'Transportation'
        }[col]

    def get_eol_label(self, col):
        return {
            'eolRecyclingPercent': 'recycling',
            'eolIncinerationPercent': 'incineration',
            'eolLandfillPercent': 'landfill',
            'eolMismanagedPercent': 'mismanaged'
        }[col]

    def get_trade_label(self, col):
        return {
            'netImportArticlesMT': 'articles',
            'netImportFibersMT': 'fibers',
            'netImportGoodsMT': 'goods',
            'netImportResinMT': 'resin'
        }[col]
    
    def requires(self):
        return CheckSeedCurveProjectionTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '506_project_curve_raw.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        return 'project_curve'

    def get_consumption_model_filename(self):
        return 'consumption_curve.pickle'

    def get_waste_model_filename(self):
        return 'waste_curve.pickle'

    def get_trade_model_filename(self):
        return 'trade_curve.pickle'

    def get_waste_trade_model_filename(self):
        return 'wasteTrade_curve.pickle'

    def hot_encode(self, candidate, hot_value):
        return 1 if candidate == hot_value else 0

    def get_consumption_inputs_sql(self, year, region, sector):
        template_vals = {
            'table_name': self.get_table_name(),
            'year': year,
            'region': region,
            'label': self.get_sector_label(sector)
        }

        return '''
            SELECT
                year,
                region,
                population,
                gdp,
                '{label}' AS majorMarketSector
            FROM
                {table_name}
            WHERE
                year = {year}
                AND region = '{region}'
        '''.format(**template_vals)

    def get_waste_inputs_sql(self, year, region, type_name):
        template_vals = {
            'table_name': self.get_table_name(),
            'year': year,
            'region': region,
            'label': self.get_eol_label(type_name)
        }

        return '''
            SELECT
                year,
                region,
                population,
                gdp,
                '{label}' AS type
            FROM
                {table_name}
            WHERE
                year = {year}
                AND region = '{region}'
        '''.format(**template_vals)

    def get_trade_inputs_sql(self, year, region, type_name):
        template_vals = {
            'table_name': self.get_table_name(),
            'year': year,
            'region': region,
            'label': self.get_trade_label(type_name)
        }

        return '''
            SELECT
                year,
                region,
                population,
                gdp,
                '{label}' AS type
            FROM
                {table_name}
            WHERE
                year = {year}
                AND region = '{region}'
        '''.format(**template_vals)

    def get_waste_trade_inputs_sql(self, year, region, type_name):
        template_vals = {
            'table_name': self.get_table_name(),
            'year': year,
            'region': region
        }

        return '''
            SELECT
                year,
                region,
                population,
                gdp
            FROM
                {table_name}
            WHERE
                year = {year}
                AND region = '{region}'
        '''.format(**template_vals)
    
    def get_consumption_inputs_cols(self):
        return [
            'year',
            'region',
            'population',
            'gdp',
            'majorMarketSector'
        ]
    
    def get_waste_inputs_cols(self):
        return [
            'year',
            'region',
            'population',
            'gdp',
            'type'
        ]
    
    def get_trade_inputs_cols(self):
        return [
            'year',
            'region',
            'population',
            'gdp',
            'type'
        ]

    def get_waste_trade_inputs_cols(self):
        return [
            'year',
            'region',
            'population',
            'gdp'
        ]


class SeedNaiveProjectionTask(tasks_project_template.SeedProjectionTask):
    """Prepare the projections table for "naive" model projections."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    
    def requires(self):
        return PreCheckNaiveProjectTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '507_seed_naive.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        return 'project_naive'


class CheckSeedNaiveProjectionTask(tasks_project_template.CheckSeedProjectionTask):
    """Confirm the projections table is built for "naive" model projections."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    
    def requires(self):
        return SeedNaiveProjectionTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '508_check_seed_naive.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        return 'project_naive'


class ProjectNaiveRawTask(ProjectCurveRawTask):
    """Perform projections using the set of related "naive" models."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    
    def requires(self):
        return CheckSeedNaiveProjectionTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '509_project_naive_raw.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        return 'project_naive'

    def get_consumption_model_filename(self):
        return 'consumption_curve_naive.pickle'

    def get_waste_model_filename(self):
        return 'waste_curve_naive.pickle'

    def get_trade_model_filename(self):
        return 'trade_curve_naive.pickle'

    def get_waste_trade_model_filename(self):
        return 'wasteTrade_curve_naive.pickle'


class NormalizeMlTask(tasks_project_template.NormalizeProjectionTask):
    """Perform normalization tasks on the machine learning projections."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    
    def requires(self):
        return ProjectMlRawTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '510_normalize_ml.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        return 'project_ml'


class NormalizeCurveTask(tasks_project_template.NormalizeProjectionTask):
    """Perform normalization tasks on the curve model projections."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    
    def requires(self):
        return ProjectCurveRawTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '511_normalize_curve.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        return 'project_curve'


class NormalizeNaiveTask(tasks_project_template.NormalizeProjectionTask):
    """Perform normalization tasks on the naive model projections."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    
    def requires(self):
        return ProjectNaiveRawTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '512_normalize_naive.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        return 'project_naive'


class CheckNormalizeMlTask(tasks_project_template.NormalizeCheckTask):
    """Check normalization was successful on the machine learning projections."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    
    def requires(self):
        return NormalizeMlTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '513_check_normalize_ml.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        return 'project_ml'

    def should_assert_waste_trade_min(self):
        return True

    def should_assert_trade_max(self):
        return True


class CheckNormalizeCurveTask(tasks_project_template.NormalizeCheckTask):
    """Check normalization was successful on the curve model projections."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    
    def requires(self):
        return NormalizeCurveTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '514_check_normalize_curve.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        return 'project_curve'


class ApplyLifecycleMLTask(tasks_project_template.ApplyLifecycleTask):
    """Apply lifetime / lifecycle distributions to determine waste in ML projections."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    
    def requires(self):
        return CheckNormalizeMlTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '516_lifecycle_ml.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        return 'project_ml'


class ApplyLifecycleCurveTask(tasks_project_template.ApplyLifecycleTask):
    """Apply lifetime / lifecycle distributions to determine waste in curve projections."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    
    def requires(self):
        return CheckNormalizeCurveTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '517_lifecycle_curve.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        return 'project_curve'


class ApplyLifecycleNaiveTask(tasks_project_template.ApplyLifecycleTask):
    """Apply lifetime / lifecycle distributions to determine waste in naive projections."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    
    def requires(self):
        return NormalizeNaiveTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '518_lifecycle_naive.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        return 'project_naive'


class MlLifecycleCheckTask(tasks_project_template.LifecycleCheckTask):
    """Check that lifecycle / lifetime distributions were applied to the ML projections."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    
    def requires(self):
        return ApplyLifecycleMLTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '519_check_lifecycle_ml.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        return 'project_ml'


class CurveLifecycleCheckTask(tasks_project_template.LifecycleCheckTask):
    """Check that lifecycle / lifetime distributions were applied to the curve projections."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    
    def requires(self):
        return ApplyLifecycleCurveTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '520_check_lifecycle_curve.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        return 'project_curve'


class MlApplyWasteTradeTask(tasks_project_template.ApplyWasteTradeProjectionTask):
    """Apply the effects of waste trade on other summary stats in machine learning projections."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    
    def requires(self):
        return MlLifecycleCheckTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '521_ml_apply_waste_trade.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        return 'project_ml'


class CurveApplyWasteTradeTask(tasks_project_template.ApplyWasteTradeProjectionTask):
    """Apply the effects of waste trade on other summary stats in curve model projections."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    
    def requires(self):
        return CurveLifecycleCheckTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '522_curve_apply_waste_trade.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        return 'project_curve'


class NaiveApplyWasteTradeTask(tasks_project_template.ApplyWasteTradeProjectionTask):
    """Apply the effects of waste trade on other summary stats in naive projections."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)
    
    def requires(self):
        return ApplyLifecycleNaiveTask(task_dir=self.task_dir)

    def output(self):
        out_path = os.path.join(self.task_dir, '523_naive_apply_waste_trade.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        return 'project_naive'
