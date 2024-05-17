"""Simple pre-deploy checks for pipeline outputs before forwarding to the tool.

Standalone script for simple pre-deploy checks for pipeline outputs before forwarding to the tool.

License:
    BSD, see LICENSE.md
"""

import csv
import sys

import const


NUM_ARGS = 1
USAGE_STR = 'python check_output.py [target]'

ATTRS_EXPECTED = [
    'consumptionAgricultureMT',
    'consumptionConstructionMT',
    'consumptionElectronicMT',
    'consumptionHouseholdLeisureSportsMT',
    'consumptionPackagingMT',
    'consumptionTransportationMT',
    'consumptionTextileMT',
    'consumptionOtherMT',
    'eolRecyclingMT',
    'eolLandfillMT',
    'eolIncinerationMT',
    'eolMismanagedMT',
    'netImportsMT',
    'netExportsMT',
    'domesticProductionMT',
    'netWasteExportMT',
    'netWasteImportMT'
]

REGIONS_EXPECTED = const.REGIONS

YEARS_REQUIRED = set(range(2010, 2050))


class CheckResult:
    """Simple structure to describe if the checks were successful."""

    def __init__(self, successful, message):
        """Create a new result record.

        Args:
            successful: Boolean flag with True for success and False otherwise.
            message: Additional human-readable description of the check outcome including
                information about issues encountered or paths to resolution.
        """
        self._successful = successful
        self._message = message

    def get_successful(self):
        """Get a flag indicating if the checks were successful.

        Returns:
            True if successful and False otherwise.
        """
        return self._successful

    def get_message(self):
        """Get a description of why the success or failure criteria were met.

        Returns:
            Additional human-readable description of the check outcome including information about
            issues encountered or paths to resolution. This may be a confirmation message.
        """
        return self._message


def has_attrs(rows):
    """Determine if a collection of rows has all anticipated attributes.

    Args:
        rows: Collection of dictionaries where each dictionary is a record to be checked.

    Returns:
        True if all rows had the expected attributes and False otherwise.
    """
    row = rows[0]
    missing_attrs = filter(lambda x: x not in row, ATTRS_EXPECTED)
    num_mismatched = sum(map(lambda x: 1, missing_attrs))
    return num_mismatched == 0


def has_regions(rows):
    """Determine if a collection of rows has all anticipated geographic regions.

    Args:
        rows: Collection of dictionaries where each dictionary is a record to be checked.

    Returns:
        True if all regions are found and False otherwise.
    """
    regions_found = set(map(lambda x: x['region'], rows))
    regions_expected = set(REGIONS_EXPECTED)
    difference = regions_found.symmetric_difference(regions_expected)
    return len(difference) == 0


def has_years(rows):
    """Determine if a collection of rows reports on the anticipated years.

    Confirm that outputs include records for each "expected" year as described in YEARS_REQUIRED.
    These are the years expected by the tool.

    Args:
        rows: Collection of dictionaries where each dictionary is a record to be checked.

    Returns:
        True if all years are found and False otherwise.
    """
    years_found = set(map(lambda x: int(x['year']), rows))
    for year in YEARS_REQUIRED:
        if year not in years_found:
            print('Missing year %d' % year)
            return False

    return True


def check(loc):
    """Check an output file.

    Args:
        loc: The path to the output file whose rows are to be checked.

    Returns:
        A CheckResult describing if the file passed checks or not.
    """
    with open(loc) as f:
        rows = list(csv.DictReader(f))

    if not has_attrs(rows):
        return CheckResult(False, 'Columns missing.')

    if not has_regions(rows):
        return CheckResult(False, 'Unexpected regions.')

    if not has_years(rows):
        return CheckResult(False, 'Unexpected years.')

    return CheckResult(True, 'Passed checks.')


def main():
    """Main entry point for the command-line script."""
    if len(sys.argv) != NUM_ARGS + 1:
        print(USAGE_STR)
        sys.exit(1)

    loc = sys.argv[1]

    result = check(loc)

    if not result.get_successful():
        raise RuntimeError('Failed with: ' + result.get_message())


if __name__ == '__main__':
    main()
