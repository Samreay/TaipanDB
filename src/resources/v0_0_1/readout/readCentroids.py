import logging
import sys
import os
from ....scripts.extract import extract_from
from taipan.core import TaipanTile


def execute(cursor):
    logging.info('Reading tile centroids from database')

    centroids_db = extract_from(cursor, 'field',
                                columns=['field_id', 'ra', 'dec', 'ux', 'uy', 'uz'])

    return_objects = [TaipanTile(c['ra'], c['dec'], field_id=c['field_id'])
                      for c in centroids_db]

    logging.info('Extracted %d centroids from database' % centroids_db.shape[0])
    return return_objects