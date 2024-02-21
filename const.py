"""Shared constants for the plastics ML training pipeline.

License:
    BSD, see LICENSE.md
"""

import os
import pathlib
import random


# Define seed
SEED = 12345
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
    'consumptionTransportationMT',
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
    'consumptionAgricultureMT': {
        'mean': 2,
        'std': 1,
        'mu': 0.58157540490284,
        'sigma': 0.472380727077439
    },
    'consumptionConstructionMT': {
        'mean': 35,
        'std': 7,
        'mu': 3.53573770491277,
        'sigma': 0.198042200435365,
    },
    'consumptionElectronicMT': {
        'mean': 8,
        'std': 2,
        'mu': 2.04912923077162,
        'sigma': 0.24622067706924
    },
    'consumptionHouseholdLeisureSportsMT': {
        'mean': 3,
        'std': 1,
        'mu': 1.0459320308392,
        'sigma': 0.324592845974501
    },
    'consumptionPackagingMT': {
        'mean': 0.5,
        'std': 0.1,
        'mu': -0.712757537136586,
        'sigma': 0.198042200435365
    },
    'consumptionTransportationMT': {
        'mean': 13,
        'std': 3,
        'mu': 2.53900693977703,
        'sigma': 0.22778242989531
    },
    'consumptionTextileMT': {
        'mean': 5,
        'std': 1.5,
        'mu': 1.56634906431357,
        'sigma': 0.293560379208524
    },
    'consumptionOtherMT': {
        'mean': 5,
        'std': 1.5,
        'mu': 1.56634906431357,
        'sigma': 0.293560379208524
    }
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
    'consumptionTransportationMT',
    'consumptionTextileMT',
    'consumptionOtherMT',
    'netImportsMT',
    'netExportsMT',
    'domesticProductionMT',
    'netWasteExportMT',
    'netWasteImportMT'
]

CIRCULARITY_LOOPS = 31
