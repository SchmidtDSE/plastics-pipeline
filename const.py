import os
import pathlib
import random


SEED = 1234
random.seed(SEED)

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

LIFECYCLE_DISTRIBUTIONS = {
    'consumptionAgricultureMT': {'mean': 2, 'std': 1},
    'consumptionConstructionMT': {'mean': 35, 'std': 7},
    'consumptionElectronicMT': {'mean': 8, 'std': 2},
    'consumptionHouseholdLeisureSportsMT': {'mean': 3, 'std': 1},
    'consumptionPackagingMT': {'mean': 0.5, 'std': 0.1},
    'consumptionTransporationMT': {'mean': 13, 'std': 3},
    'consumptionTextileMT': {'mean': 5, 'std': 1.5},
    'consumptionOtherMT': {'mean': 5, 'std': 1.5}
}

EXPORT_FIELD_NAMES = [
    'year',
    'region',
    'eolRecyclingMT',
    'eolLandfillMT',
    'eolIncinerationMT',
    'eolMismanagedMT',
    'consumptionAgricultureMT',
    'consumptionConstructionMT',
    'consumptionElectronicMT',
    'consumptionHouseholdLeisureSportsMT',
    'consumptionPackagingMT',
    'consumptionTransporationMT',
    'consumptionTextileMT',
    'consumptionOtherMT',
    'netImportsMT',
    'netExportsMT',
    'domesticProductionMT'
]
