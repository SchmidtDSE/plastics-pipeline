"""Utility for working with SQL files.

License:
    BSD, see LICENSE.md
"""
import os

import jinja2

import const


def get_sql_file(filename, sql_dir=None, target_dir=None, additional_params=None):
    """Get contents of a SQL file with jinja applied.

    Args:
        filename: The name of the SQL file.
        sql_dir: Optional subdirectory within the sql directory where this script can be found.
        target_dir: Directory prefix to use or None if not required.
        additional_params: Dictionary with additional jinja template values or None if no additional
            params available.

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
    
    all_params = {
        'regions': const.REGIONS_INFO,
        'target_dir': target_dir
    }
    if additional_params:
        all_params.update(additional_params)

    with open(template_path) as f:
        template = jinja2.Environment(loader=jinja2.BaseLoader()).from_string(f.read())
        rendered = template.render(**all_params)

    return rendered
