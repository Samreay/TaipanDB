import logging
from scripts.create import extract_from
from taipan.core import TaipanTarget

def execute(cursor):
    logging.info('Reading guides from database')

    guides_db = extract_from(cursor, 'target', conditions=[
        ('is_guide', True),
        ],
        columns=['target_id', 'ra', 'dec'])

    return_objects = [TaipanTarget(
        g['target_id'], g['ra'], g['dec'], guide=True
        ) for g in guides_db]

    logging.info('Extracted %d guides from database' % guides_db.shape[0])
    return return_objects