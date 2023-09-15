import json
import os
import pickle
import sqlite3
import statistics

import luigi

import const
import tasks_curve
import tasks_ml
import tasks_sql


class SeedProjectionTask(tasks_sql.SqlExecuteTask):

    def transform_sql(self, sql_contents):
        return sql_contents.format(table_name=self.get_table_name())

    def get_scripts(self):
        return ['08_project/build_model_table.sql']

    def get_table_name(self):
        raise NotImplementedError('Use implementor.')


class CheckSeedProjectionTask(tasks_sql.SqlCheckTask):

    def get_table_name(self):
        raise NotImplementedError('Use implementor.')


class ProjectRawTask(luigi.Task):
    
    def run(self):
        with self.input().open('r') as f:
            job_info = json.load(f)

        consumption_model = self.get_model(
            self.get_consumption_model_filename(),
            job_info
        )
        waste_model = self.get_model(
            self.get_waste_model_filename(),
            job_info
        )
        trade_model = self.get_model(
            self.get_trade_model_filename(),
            job_info
        )

        database_loc = job_info['database']
        connection = sqlite3.connect(database_loc)

        for year in range(2021, 2051):
            for region in ['china', 'eu30', 'nafta', 'row']:
                self.project(
                    connection,
                    year,
                    region,
                    consumption_model,
                    waste_model,
                    trade_model
                )

        connection.close()

        with self.output().open('w') as f:
            return json.dump(job_info, f)

    def project(self, connection, year, region, consumption_model, waste_model, trade_model):
        updated_output_row = {
            'table_name': self.get_table_name(),
            'year': year,
            'region': region
        }
        
        updated_output_row.update(self.get_consumption_projections(
            connection,
            year,
            region,
            consumption_model
        ))

        updated_output_row.update(self.get_waste_projections(
            connection,
            year,
            region,
            waste_model
        ))

        updated_output_row.update(self.get_trade_projections(
            connection,
            year,
            region,
            trade_model
        ))

        cursor = connection.cursor()

        cursor.execute('''
            UPDATE
                {table_name}
            SET
                consumptionAgricultureMT = {consumptionAgricultureMT}
                consumptionConstructionMT = {consumptionConstructionMT}
                consumptionElectronicMT = {consumptionElectronicMT}
                consumptionHouseholdLeisureSportsMT = {consumptionHouseholdLeisureSportsMT}
                consumptionOtherMT = {consumptionOtherMT}
                consumptionPackagingMT = {consumptionPackagingMT}
                consumptionTextileMT = {consumptionTextileMT}
                consumptionTransporationMT = {consumptionTransporationMT}
                eolRecyclingPercent = {eolRecyclingPercent}
                eolIncinerationPercent = {eolIncinerationPercent}
                eolLandfillPercent = {eolLandfillPercent}
                eolMismanagedPercent = {eolMismanagedPercent}
                netImportArticlesMT = {netImportArticlesMT}
                netImportFibersMT = {netImportFibersMT}
                netImportGoodsMT = {netImportGoodsMT}
                netImportResinMT = {netImportResinMT}
            WHERE
                year = {year}
                AND region = {region}
        '''.format(updated_output_row))

        connection.commit()
        cursor.close()

    def get_model(self, filename, job_info):
        model_loc = os.path.join(
            job_info['directories']['workspace'],
            filename
        )
        with open(model_loc, 'rb') as f:
            return pickle.load(f)['model']

    def build_instances(self, connection, sql, cols):
        cursor = connection.cursor()
        print(sql)
        cursor.execute(sql)
        results_flat = cursor.fetchall()
        cursor.close()

        results_keyed = [dict(zip(cols, results)) for result in results_flat]
        return results_keyed

    def get_consumption_projections(self, connection, year, region, consumption_model):
        return self.get_projections(
            connection,
            year,
            region,
            consumption_model,
            [
                'consumptionAgricultureMT',
                'consumptionConstructionMT',
                'consumptionElectronicMT',
                'consumptionHouseholdLeisureSportsMT',
                'consumptionOtherMT',
                'consumptionPackagingMT',
                'consumptionTextileMT',
                'consumptionTransporationMT'
            ],
            lambda x: self.get_consumption_inputs_sql(year, region, x),
            lambda: self.get_consumption_inputs_cols(),
            lambda instance, prediction: self.transform_consumption_prediction(
                instance,
                prediction
            )
        )

    def get_waste_projections(self, connection, year, region, waste_model):
        return self.get_projections(
            connection,
            year,
            region,
            waste_model,
            [
                'eolRecyclingPercent',
                'eolIncinerationPercent',
                'eolLandfillPercent',
                'eolMismanagedPercent'
            ],
            lambda x: self.get_waste_inputs_sql(year, region, x),
            lambda: self.get_waste_inputs_cols(),
            lambda instance, prediction: self.transform_consumption_prediction(
                instance,
                prediction
            )
        )

    def get_trade_projections(self, connection, year, region, trade_model):
        return self.get_projections(
            connection,
            year,
            region,
            trade_model,
            [
                'netImportArticlesMT',
                'netImportFibersMT',
                'netImportGoodsMT',
                'netImportResinMT'
            ],
            lambda x: self.get_trade_inputs_sql(year, region, x),
            lambda: self.get_trade_inputs_cols(),
            lambda instance, prediction: self.transform_consumption_prediction(
                instance,
                prediction
            )
        )

    def get_projections(self, connection, year, region, model, keys, sql_getter, cols_getter,
            prediction_transformer):

        def build_instances(label):
            sql = sql_getter(label)
            cols = cols_getter()
            return self.build_instances(connection, sql, cols)

        instances_by_key = dict(map(
            lambda x: (x, build_instances(x)),
            keys
        ))

        predictions = {}
        for key, instances in instances_by_key.items():
            raw_predictions = model.predict(instances)
            transformed_predictions = map(
                lambda x: prediction_transformer(x[0], x[1]),
                zip(instances, raw_predictions)
            )
            prediction = statistics.mean(transformed_predictions)
            predictions[key] = prediction

        return predictions

    def get_table_name(self):
        raise NotImplementedError('Use implementor.')

    def get_consumption_model_filename(self):
        raise NotImplementedError('Use implementor.')

    def get_waste_model_filename(self):
        raise NotImplementedError('Use implementor.')

    def get_trade_model_filename(self):
        raise NotImplementedError('Use implementor.')

    def get_consumption_inputs_sql(self, year, region, sector):
        raise NotImplementedError('Use implementor.')

    def get_waste_inputs_sql(self, year, region, type_name):
        raise NotImplementedError('Use implementor.')

    def get_trade_inputs_sql(self, year, region, type_name):
        raise NotImplementedError('Use implementor.')
    
    def get_consumption_inputs_cols(self):
        raise NotImplementedError('Use implementor.')
    
    def get_waste_inputs_cols(self):
        raise NotImplementedError('Use implementor.')
    
    def get_trade_inputs_cols(self):
        raise NotImplementedError('Use implementor.')

    def transform_consumption_prediction(self, instance, prediction):
        raise NotImplementedError('Use implementor.')

    def transform_waste_prediction(self, instance, prediction):
        raise NotImplementedError('Use implementor.')

    def transform_trade_prediction(self, instance, prediction):
        raise NotImplementedError('Use implementor.')
