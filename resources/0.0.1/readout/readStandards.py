import logging
from scripts.create import extract_from
from taipan.core import TaipanTarget

def execute(cursor):
    logging.info('Reading standards from database')

    standards_db = extract_from(cursor, 'target', conditions=[
        ('is_standard', True),
        ],
        columns=['target_id', 'ra', 'dec'])

    return_objects = [TaipanTarget(
        s['target_id'], s['ra'], s['dec'], standard=True
        ) for s in standards_db]

    logging.info('Extracted %d standards from database' % standards_db.shape[0])
    return return_objects