import os
import pathlib


PARENT_DIR = pathlib.Path(__file__).parent.absolute()
DEFAULT_TASK_DIR = os.path.join(PARENT_DIR, 'task')
SQL_DIR = os.path.join(PARENT_DIR, 'sql')

PREPROC_FIELD_NAMES = [
    'year',
    'region',
    'inputProduceFiberMT',
    'inputProduceResinMT',
    'inputImportResinMT',
    'inputImportArticlesMT',
    'inputImportGoodsMT',
    'inputImportFiberMT',
    'inputAdditivesMT',
    'netImportArticlesMT',
    'netImportFibersMT',
    'netImportGoodsMT',
    'netImportResinMT',
    'consumptionAgricultureMT',
    'consumptionConstructionMT',
    'consumptionElectronicMT',
    'consumptionHouseholdLeisureSportsMT',
    'consumptionPackagingMT',
    'consumptionTransporationMT',
    'consumptionTextileMT',
    'consumptionOtherMT',
    'eolRecyclingPercent',
    'eolIncinerationPercent',
    'eolLandfillPercent',
    'eolMismanagedPercent'
]

AUX_FIELD_NAMES = [
    'year',
    'region',
    'population',
    'gdp'
]
