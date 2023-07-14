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
    'consumptionTextitleMT',
    'consumptionOtherMT',
    'eolRecyclingMT',
    'eolLandfillMT',
    'eolIncinerationMT',
    'eolMismanagedMT'
]

REGIONS_EXPECTED = [
    'china',
    'eu30',
    'nafta',
    'row',
    'global'
]

YEARS_EXPECTED = set(range(2000, 2050))


def has_attrs(rows):
    row = rows[0]
    missing_attrs = filter(lambda x: x not in row, ATTRS_EXPECTED)
    num_missing = sum(map(lambda x: 1, missing_attrs))
    return num_missing == 0


def has_regions(rows):
    regions_found = set(map(lambda x: x['region'], rows))
    regions_expected = set(REGIONS_EXPECTED)
    difference = regions_found.symmetric_difference(regions_expected)
    return len(difference) == 0


def has_years(rows):
    years_found = set(map(lambda x: int(x['year']), rows))
    difference = YEARS_EXPECTED.symmetric_difference(years_found)
    return len(difference) == 0


def main():
    if len(sys.argv) != NUM_ARGS + 1:
        print(USAGE_STR)
        sys.exit(1)

    loc = sys.argv[1]

    with open(loc) as f:
        rows = list(csv.DictReader(f))

    if not has_attrs(rows):
        print('Columns missing.')
        sys.exit(1)

    if not has_regions(rows):
        print('Unexpected regions.')
        sys.exit(1)

    if not has_years(rows):
        print('Unexpected years.')
        sys.exit(1)

    print('Passed checks.')
    sys.exit(0)


if __name__ == '__main__':
    main()
