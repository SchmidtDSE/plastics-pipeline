"""Tasks to load auxiliary data required for modeling like GDP and population.

License:
    BSD, see LICENSE.md
"""

import csv
import json
import os
import sqlite3
import statistics
import subprocess

import luigi

import const
import tasks_sql
import tasks_workspace


class ProcessGdpTask(luigi.Task):
    """Task to prepare GDP data before its use in modeling."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        """Require that files are in the workspace with expected filenames."""
        return tasks_workspace.CleanFilenamesTask(task_dir=self.task_dir)

    def output(self):
        """Report that GDP data are available for modeling in CSV files."""
        out_path = os.path.join(self.task_dir, '100_raw_gdp.json')
        return luigi.LocalTarget(out_path)

    def run(self):
        """Preprocess auxiliary data for GDP."""
        with self.input().open('r') as f:
            job_info = json.load(f)

        workspace_dir = job_info['directories']['workspace']

        with open(os.path.join(workspace_dir, 'a3regions.json')) as f:
            region_mapping = json.load(f)

        aggregator = {}
        with open(os.path.join(workspace_dir, 'a1gdpraw.csv')) as f:
            reader = csv.DictReader(f)

            location_key = None

            for row in reader:

                if location_key is None:
                    options = filter(lambda x: 'LOCATION' in x, row.keys())
                    location_key = list(options)[0]

                iso_code = row[location_key]
                year_str = row['TIME']
                value_str = row['Value']

                region = region_mapping.get(iso_code, 'row').lower()
                year = int(year_str.strip())
                value = float(value_str.strip())

                key = '{region}.{year}'.format(region=region, year=year)
                if key not in aggregator:
                    aggregator[key] = {
                        'region': region,
                        'year': year,
                        'values': []
                    }

                aggregator[key]['values'].append(value / 1000000 * 1000)

        output_rows_agg = aggregator.values()
        output_rows = map(lambda x: {
            'region': x['region'],
            'year': x['year'],
            'gdp': statistics.median(x['values']),
            'gdpSum': sum(x['values'])
        }, output_rows_agg)

        with open(os.path.join(workspace_dir, 'gdpregions.csv'), 'w') as f:
            writer = csv.DictWriter(
                f,
                fieldnames=['region', 'year', 'gdp', 'gdpSum']
            )
            writer.writeheader()
            writer.writerows(output_rows)

        with self.output().open('w') as f:
            return json.dump(job_info, f)


class ProcessRawPopulationTask(luigi.Task):
    """Task to prepare population data before their use in modeling."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        """Require that files are in the workspace with expected filenames."""
        return tasks_workspace.CleanFilenamesTask(task_dir=self.task_dir)

    def output(self):
        """Report that population data are available for modeling in CSV files."""
        out_path = os.path.join(self.task_dir, '101_raw_population.json')
        return luigi.LocalTarget(out_path)

    def run(self):
        """Preprocess auxiliary data for population."""
        with self.input().open('r') as f:
            job_info = json.load(f)

        workspace_dir = job_info['directories']['workspace']

        with open(os.path.join(workspace_dir, 'a3regions.json')) as f:
            region_mapping = json.load(f)

        output_rows = {}
        with open(os.path.join(workspace_dir, 'a2populationraw.csv')) as f:
            reader = csv.DictReader(f)

            for row in reader:
                iso_code = row['ISO3 Alpha-code']
                year_str = row['Year']
                population_str = row['Total Population, as of 1 January (thousands)']

                region = region_mapping.get(iso_code, 'row').lower()
                year = int(year_str.strip())
                population = float(population_str.replace(' ', '')) / 1000

                key = '{region}.{year}'.format(region=region, year=year)
                if key not in output_rows:
                    output_rows[key] = {
                        'region': region,
                        'year': year,
                        'population': 0
                    }

                if year < 2022:
                    output_rows[key]['population'] += population

        with open(os.path.join(workspace_dir, 'a4popprojection.csv')) as f:
            reader = csv.DictReader(f)

            for row in reader:
                iso_code = row['ISO3 Alpha-code']
                year_str = row['Year']
                population_str = row['Total Population, as of 1 January (thousands)']

                region = region_mapping.get(iso_code, 'row').lower()
                year = int(year_str.strip())
                population = float(population_str.replace(' ', '')) / 1000

                key = '{region}.{year}'.format(region=region, year=year)
                if key not in output_rows:
                    output_rows[key] = {
                        'region': region,
                        'year': year,
                        'population': 0
                    }

                if year >= 2022 and year <= 2050:
                    output_rows[key]['population'] += population

        with open(os.path.join(workspace_dir, 'popregions.csv'), 'w') as f:
            writer = csv.DictWriter(
                f,
                fieldnames=['region', 'year', 'population']
            )
            writer.writeheader()
            writer.writerows(output_rows.values())

        with self.output().open('w') as f:
            return json.dump(job_info, f)


class PrepareImportFilesAuxTask(luigi.Task):
    """Tasks to prepare scripts ahead of auxiliary data import.

    Task which prepare scripts to load preprocessed auxiliary data files into the SQLite database.
    """

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        """Require that preprocessed files are available."""
        return {
            'gpd': ProcessGdpTask(task_dir=self.task_dir),
            'population': ProcessRawPopulationTask(task_dir=self.task_dir)
        }

    def output(self):
        """Report that scripts are ready to load into the SQLite database."""
        out_path = os.path.join(self.task_dir, '102_prepare_import_files.json')
        return luigi.LocalTarget(out_path)

    def run(self):
        """Build scripts to load GDP and population data into the SQLite database."""
        with self.input()['population'].open('r') as f:
            job_info = json.load(f)

        template_path = os.path.join(
            const.SQL_DIR,
            '06_aux',
            'import_files.sql'
        )

        with open(template_path) as f:
            contents = f.read()

        target_dir = job_info['directories']['workspace'] + os.path.sep

        rendered = contents.format(target_dir=target_dir)

        output_path = os.path.join(
            job_info['directories']['workspace'],
            'import_files_aux_rendered.sql'
        )

        with open(output_path, 'w') as f:
            f.write(rendered)

        with self.output().open('w') as f:
            json.dump(job_info, f)


class ExecuteImportFilesAuxTask(luigi.Task):
    """Task which imports auxiliary data files into the scratch SQLite database."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        """Require that all files required for import are ready."""
        return PrepareImportFilesAuxTask(task_dir=self.task_dir)

    def output(self):
        """Report that the files have been imported into the SQLite database."""
        out_path = os.path.join(self.task_dir, '103_execute_import_files.json')
        return luigi.LocalTarget(out_path)

    def run(self):
        """Load in the auxiliary data to the SQLite database."""
        with self.input().open('r') as f:
            job_info = json.load(f)

        sql_path = os.path.join(
            job_info['directories']['workspace'],
            'import_files_aux_rendered.sql'
        )

        db_path = job_info['database']

        command = 'cat {sql_path} | sqlite3 {db_path}'.format(
            sql_path=sql_path,
            db_path=db_path
        )

        subprocess.run(command, shell=True)

        with self.output().open('w') as f:
            json.dump(job_info, f)


class CheckImportAuxTask(tasks_sql.SqlCheckTask):
    """Check that the auxiliary data are imported successfully."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        """Require that the auxiliary data are imported."""
        return ExecuteImportFilesAuxTask(task_dir=self.task_dir)

    def output(self):
        """Report that the auxiliary data have been confirmed."""
        out_path = os.path.join(self.task_dir, '104_check_import_files.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        """Get the name of the table whose contents need to be confirmed present."""
        return 'file_popregions'


class BuildViewsAuxTask(tasks_sql.SqlExecuteTask):
    """Task which builds views that make it easier to work with auxiliary data."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        """Require that the auxiliary data have been confirmed present."""
        return CheckImportAuxTask(task_dir=self.task_dir)

    def output(self):
        """Report that the views for working with auxiliary data have been built."""
        out_path = os.path.join(self.task_dir, '105_build_views.json')
        return luigi.LocalTarget(out_path)

    def get_scripts(self):
        """Get the location of scripts needed to build the auxiliary data views."""
        return [
            '06_aux/gdp.sql',
            '06_aux/population.sql',
            '06_aux/auxiliary.sql'
        ]


class CheckViewsAuxTask(tasks_sql.SqlCheckTask):
    """Task which confirms that the auxiliary data views are avalable and populated."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        """Require that the auxiliary views have been built."""
        return BuildViewsAuxTask(task_dir=self.task_dir)

    def output(self):
        """Report that the views for the auxiliary data have been built and confirmed."""
        out_path = os.path.join(self.task_dir, '106_check_views.json')
        return luigi.LocalTarget(out_path)

    def get_table_name(self):
        """Indicate table / view to check is populated."""
        return 'auxiliary'


class BuildFrameAuxTask(luigi.Task):
    """Task which write the processed auxiliary data to a CSV file."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        """Require that the auxiliary data view has been confirmed."""
        return CheckViewsAuxTask(task_dir=self.task_dir)

    def output(self):
        """Report that the CSV file has been exported."""
        out_path = os.path.join(self.task_dir, '107_build_frame.json')
        return luigi.LocalTarget(out_path)

    def run(self):
        """Write the auxiliary data CSV file."""
        with self.input().open('r') as f:
            job_info = json.load(f)

        database_loc = job_info['database']
        connection = sqlite3.connect(database_loc)
        cursor = connection.cursor()

        sql_filename = os.path.join(
            const.SQL_DIR,
            '07_aux_frame',
            'export.sql'
        )

        with open(sql_filename) as f:
            sql_contents = f.read()

        preprocessed_output_path = os.path.join(
            job_info['directories']['output'],
            'auxiliary.csv'
        )

        with open(preprocessed_output_path, 'w') as f:
            writer = csv.DictWriter(f, fieldnames=const.PREPROC_FIELD_NAMES)
            writer.writeheader()

            for row in cursor.execute(sql_contents):
                row_keyed = dict(zip(const.AUX_FIELD_NAMES, row))
                writer.writerow(row_keyed)

        connection.commit()

        cursor.close()
        connection.close()

        with self.output().open('w') as f:
            return json.dump(job_info, f)
