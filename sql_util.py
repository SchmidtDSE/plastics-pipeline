"""Utility for working with SQL files.

License:
    BSD, see LICENSE.md
"""
import os

import jinja2

import const


def get_sql_file(filename, sql_dir=None):
    """Get contents of a SQL file with jinja applied.

    Args:
        filename: The name of the SQL file.
        sql_dir: Optional subdirectory within the sql directory where this script can be found.

    Returns:
        Contents of the requested SQL file after interpreting it as a jina template.
    """
    if sql_dir:
        template_path = os.path.join(
            const.SQL_DIR,
            sql_dir,
            filename
        )
    else:
        template_path = os.path.join(
            const.SQL_DIR,
            filename
        )

    with open(template_path) as f:
        template = jinja2.Environment(loader=jinja2.BaseLoader()).from_string(f.read())
        rendered = template.render(regions=const.REGIONS_INFO)

    return rendered
