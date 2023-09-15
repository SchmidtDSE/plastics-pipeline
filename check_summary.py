import csv
import sys

USAGE_STR = 'python check_summary_percents.py [file]'
NUM_ARGS = 1


def check(loc):
    with open(loc) as f:
        results = list(csv.DictReader(f))

    row = results[0]
    assert 'year' in row
    assert 'region' in row
    assert 'inputProduceFiberMT' in row
    assert 'inputProduceResinMT' in row
    assert 'inputImportResinMT' in row
    assert 'inputImportArticlesMT' in row
    assert 'inputImportGoodsMT' in row
    assert 'inputImportFiberMT' in row
    assert 'inputAdditivesMT' in row
    assert 'consumptionAgricultureMT' in row
    assert 'consumptionConstructionMT' in row
    assert 'consumptionElectronicMT' in row
    assert 'consumptionHouseholdLeisureSportsMT' in row
    assert 'consumptionPackagingMT' in row
    assert 'consumptionTransporationMT' in row
    assert 'consumptionTextileMT' in row
    assert 'consumptionOtherMT' in row
    assert 'eolRecyclingPercent' in row
    assert 'eolIncinerationPercent' in row
    assert 'eolLandfillPercent' in row
    assert 'eolMismanagedPercent' in row

    assert len(results) > 50


def main():
    if len(sys.argv) != NUM_ARGS + 1:
        print(USAGE_STR)
        sys.exit(1)

    check(sys.argv[1])


if __name__ == '__main__':
    main()
