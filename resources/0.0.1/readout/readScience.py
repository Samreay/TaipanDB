import logging
import sys
import os
sys.path.append(os.path.realpath(os.path.dirname(os.path.abspath(__file__)) + "/../../.."))
from scripts.extract import extract_from_joined
from taipan.core import TaipanTarget

def execute(cursor):
    logging.info('Reading guides from database')

    targets_db = extract_from_joined(cursor, ['target', 'science_target'],
        conditions=[('is_science', True)],
        columns=['target_id', 'ra', 'dec', 'ux', 'uy', 'uz', 'priority'])

    return_objects = [TaipanTarget(
        g['target_id'], g['ra'], g['dec'], priority=g['priority'],
        ucposn=(g['ux'], g['uy'], g['uz']),
        ) for g in targets_db]

    logging.info('Extracted %d targets from database' % return_objects.shape[0])
    return return_objects
