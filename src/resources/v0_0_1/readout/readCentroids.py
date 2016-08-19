import logging
import sys
import os
from ....scripts.extract import extract_from
from taipan.core import TaipanTile


def execute(cursor, field_ids=None):
    """
    Read centroid (i.e. field) parameters from the database.

    Parameters
    ----------
    cursor:
        psycopg2 cursor for interacting with the database.
    field_ids:
        Optional; list of field IDs to return from the database. Defaults to
        None, at which point all field centroids are returned.

    Returns
    -------
    return_objects:
        A list of (empty) TaipanTile objects. One TaipanTile is returned for
        each field requested, or for all fields in the database if no
        list of requested fields was passed.
    """
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
