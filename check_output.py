import csv
import sys

NUM_ARGS = 1
USAGE_STR = 'python check_output.py [target]'

ATTRS_EXPECTED = [
    'consumptionAgricultureMT',
    'consumptionConstructionMT',
    'consumptionElectronicMT',
    'consumptionHouseholdLeisureSportsMT',
    'consumptionPackagingMT',
    'consumptionTransporationMT',
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

REGIONS_EXPECTED = [
    'china',
    'eu30',
    'nafta',
    'row'
]

YEARS_REQUIRED = set(range(2010, 2050))


class CheckResult:

    def __init__(self, successful, message):
        self._successful = successful
        self._message = message

    def get_successful(self):
        return self._successful

    def get_message(self):
        return self._message


def has_attrs(rows):
    row = rows[0]
    missing_attrs = filter(lambda x: x not in row, ATTRS_EXPECTED)
    num_mismatched = sum(map(lambda x: 1, missing_attrs))
    return num_mismatched == 0


def has_regions(rows):
    regions_found = set(map(lambda x: x['region'], rows))
    regions_expected = set(REGIONS_EXPECTED)
    difference = regions_found.symmetric_difference(regions_expected)
    return len(difference) == 0


def has_years(rows):
    years_found = set(map(lambda x: int(x['year']), rows))
    for year in YEARS_REQUIRED:
        if year not in years_found:
            print('Missing year %d' % year)
            return False

    return True


def check(loc):
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
    if len(sys.argv) != NUM_ARGS + 1:
        print(USAGE_STR)
        sys.exit(1)

    loc = sys.argv[1]

    result = check(loc)
    
    if not result.get_successful():
        raise RuntimeError('Failed with: ' + result.get_message())


if __name__ == '__main__':
    main()
