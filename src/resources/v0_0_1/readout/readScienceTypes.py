import logging
import sys
import os
from ....scripts.extract import extract_from
from taipan.core import TaipanTarget


def execute(cursor, target_ids=None):
    logging.info('Reading science targets (types) from database')

    if target_ids is None:
        target_ids = []

    targets_db = extract_from(cursor, 'science_target',
                              columns=['target_id', 'is_vpec_target',
                                       'is_H0_target', 'is_lowz_target'])

    logging.info('Extracted %d targets from database' % len(targets_db))
    return targets_db
