import csv
import sys

USAGE_STR = 'python check_summary_percents.py [file]'
NUM_ARGS = 1


def main():
    if len(sys.argv) != NUM_ARGS + 1:
        print(USAGE_STR)
        sys.exit(1)

    with open(sys.argv[1]) as f:
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
    assert 'consumptionAgriculturePercent' in row
    assert 'consumptionConstructionPercent' in row
    assert 'consumptionElectronicPercent' in row
    assert 'consumptionHouseholdLeisureSportsPercent' in row
    assert 'consumptionPackagingPercent' in row
    assert 'consumptionTransporationPercent' in row
    assert 'consumptionTextitlePercent' in row
    assert 'consumptionOtherPercent' in row
    assert 'eolRecyclingPercent' in row
    assert 'eolIncinerationPercent' in row
    assert 'eolLandfillPercent' in row
    assert 'eolMismanagedPercent' in row

    assert len(results) > 50


if __name__ == '__main__':
    main()
