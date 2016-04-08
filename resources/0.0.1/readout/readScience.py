import logging
from scripts.create import extract_from
from taipan.core import TaipanTarget

def execute(cursor):
    logging.info('Reading guides from database')

    targets_db = extract_from_join(cursor, ['target', 'science_target'],
        conditions=[('is_science', True)]
        columns=['target_id', 'ra', 'dec', 'ux', 'uy', 'uz', 'priority'])

    return_objects = [TaipanTarget(
        g['target_id'], g['ra'], g['dec'], priority=g['priority'],
        ucposn=(g['ux'], g['uy'], g['uz']),
        ) for g in targets_db]

    logging.info('Extracted %d targets from database' % guides_db.shape[0])
    return return_objects