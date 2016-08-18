import logging
import sys
import os
from ....scripts.extract import extract_from
from taipan.core import TaipanTile


def execute(cursor, field_ids=None):
    logging.info('Reading tile centroids from database')

    if field_ids is None:
        conditions = None
    else:
        field_ids = list(field_ids)
        conditions = [('field_id', 'IN', field_ids)]

    centroids_db = extract_from(cursor, 'field',
                                columns=['field_id', 'ra', 'dec',
                                         'ux', 'uy', 'uz'],
                                conditions=conditions)

    return_objects = [TaipanTile(c['ra'], c['dec'], field_id=c['field_id'],
                                 ucposn=[c['ux'], c['uy'], c['uz']])
                      for c in centroids_db]

    logging.info('Extracted %d centroids from database' % centroids_db.shape[0])
    return return_objects
