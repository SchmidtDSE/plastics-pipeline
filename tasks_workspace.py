"""Tasks related to establishing a workspace in which the rest of the pipeline operates.

License:
    BSD, see LICENSE.md
"""

import json
import os
import shutil

import luigi

import clean_filenames
import const


class PrepareWorkspaceTask(luigi.Task):
    """Scaffold the directories required by the workspace and copy data files.

    Task which creates the directories required by this pipeline and copies out data files to
    prevent any damage during the pipeline's execution.
    """

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def output(self):
        """Indicate that the scaffolding has been set up."""
        out_path = os.path.join(self.task_dir, '001_task_start.json')
        return luigi.LocalTarget(out_path)

    def run(self):
        """Setup the scaffolding."""
        with open(os.path.join(self.task_dir, 'job.json')) as f:
            job_info = json.load(f)

        if not os.path.exists(job_info['directories']['output']):
            os.makedirs(job_info['directories']['output'])

        shutil.copytree(
            job_info['directories']['data'],
            job_info['directories']['workspace'],
            dirs_exist_ok=True
        )

        with self.output().open('w') as f:
            json.dump(job_info, f)


class CleanFilenamesTask(luigi.Task):
    """Clean filenames for input data to avoid any problematic characters."""

    task_dir = luigi.Parameter(default=const.DEFAULT_TASK_DIR)

    def requires(self):
        """Require that the workspace have been established."""
        return PrepareWorkspaceTask(task_dir=self.task_dir)

    def output(self):
        """Indicate that filenames have been cleaned."""
        out_path = os.path.join(self.task_dir, '002_task_clean_files.json')
        return luigi.LocalTarget(out_path)

    def run(self):
        """Clean filenames."""
        with self.input().open('r') as f:
            job_info = json.load(f)

        clean_filenames.execute(job_info['directories']['workspace'])

        if const.USE_PREFORMATTED:
            sample_file = os.path.join(
                job_info['directories']['workspace'],
                'output',
                'combined.db'
            )
        else:
            sample_file = os.path.join(
                job_info['directories']['workspace'],
                '01productionofresinnofiber.csv'
            )

        assert os.path.exists(sample_file)

        with self.output().open('w') as f:
            json.dump(job_info, f)
