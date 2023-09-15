import csv
import json
import os
import sqlite3

import luigi

import const


class SqlExecuteTask(luigi.Task):

    def get_scripts_resolved(self, sql_dir):
        split = map(lambda x: x.split('/'), self.get_scripts())
        return map(lambda x: os.path.join(*([sql_dir] + x)), split)

    def run(self):
        with self.input().open('r') as f:
            job_info = json.load(f)

        database_loc = job_info['database']
        connection = sqlite3.connect(database_loc)
        cursor = connection.cursor()

        sql_filenames = self.get_scripts_resolved(const.SQL_DIR)
        for filename in sql_filenames:
            with open(filename) as f:
                sql_contents = f.read()
                sql_contents = self.transform_sql(sql_contents)
                cursor.execute(sql_contents)

        connection.commit()

        cursor.close()
        connection.close()

        with self.output().open('w') as f:
            return json.dump(job_info, f)

    def transform_sql(self, sql_contents):
        return sql_contents

    def get_scripts(self):
        raise NotImplementedError('Must use implementor.')


class SqlCheckTask(luigi.Task):

    def get_table_name(self):
        raise NotImplementedError('Must use implementor.')

    def run(self):
        with self.input().open('r') as f:
            job_info = json.load(f)

        database_loc = job_info['database']
        connection = sqlite3.connect(database_loc)
        cursor = connection.cursor()

        table = self.get_table_name()
        cursor.execute('SELECT count(1) FROM {table}'.format(table=table))
        results = cursor.fetchall()
        assert results[0][0] > 0

        connection.commit()

        cursor.close()
        connection.close()

        with self.output().open('w') as f:
            return json.dump(job_info, f)