"""Template Methods for contstructing tasks which execute / check SQL statements.

License:
    BSD, see LICENSE.md
"""

import csv
import json
import os
import sqlite3

import luigi

import const


class SqlExecuteTask(luigi.Task):
    """Template Method for a Luigi Task which executes one or more SQL scripts.

    Template Method for a Luigi Task which executes one or more SQL scripts, comitting changes after
    each script.
    """

    def get_scripts_resolved(self, sql_dir):
        """Get the full path to scripts to be executed.

        Args:
            sql_dir: The path to the directory where script files can be found.

        Returns:
            List of paths for the scripts to be executed.
        """
        split = map(lambda x: x.split('/'), self.get_scripts())
        return map(lambda x: os.path.join(*([sql_dir] + x)), split)

    def run(self):
        """Execute the scripts."""
        with self.input().open('r') as f:
            job_info = json.load(f)

        database_loc = job_info['database']
        connection = sqlite3.connect(database_loc)

        sql_filenames = self.get_scripts_resolved(const.SQL_DIR)
        for filename in sql_filenames:
            cursor = connection.cursor()
            
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
        """Optional hook which can be overridden to preprocess a SQL command before its execution.

        Args:
            sql_contents: The SQL to be executed prior to preprocessing.

        Returns:
            The SQL to execute after preprocessing.
        """
        return sql_contents

    def get_scripts(self):
        """Get the list of scripts to be executed.

        Returns:
            List of strings where each is a partial path to the script to be executed.
        """
        raise NotImplementedError('Must use implementor.')


class SqlCheckTask(luigi.Task):
    """Template Method which checks that table or view has contents in it."""

    def get_table_name(self):
        """Return the name of the table to be checked.

        Returns:
            The name of the table to check.
        """
        raise NotImplementedError('Must use implementor.')

    def run(self):
        """Execute the check which, by default, simply confirms that the table is non-empty."""
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