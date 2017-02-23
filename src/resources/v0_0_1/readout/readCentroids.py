import logging
import sys
import os
from ....scripts.extract import extract_from_left_joined
from taipan.core import TaipanTile


def execute(cursor, field_ids=None, tile_list=None):
    """
    Read centroid (i.e. field) parameters from the database.

    Parameters
    ----------
    cursor:
        psycopg2 cursor for interacting with the database.
    field_ids:
        Optional; list of field IDs to return from the database. Defaults to
        None, at which point all field centroids are returned.
    tile_list : optional, list of ints
        List of tile PKs that we wish to get the field information for.
        Defaults to None, at which point all fields are returned. If both
        field_ids and tile_list are provided, field_ids takes precendence to
        avoid unexpected behaviour.

    Returns
    -------
    return_objects:
        A list of (empty) TaipanTile objects. One TaipanTile is returned for
        each field requested, or for all fields in the database if no
        list of requested fields was passed.
    """
    logging.info('Reading tile centroids from database')

    conditions = []

    if field_ids is not None:
        conditions += [('field_id', 'IN', field_ids)]
    elif tile_list is not None:
        conditions += [('tile_pk', 'IN', tile_list)]

    centroids_db = extract_from_left_joined(cursor, ['field', 'tile'],
                                            columns=['field_id', 'ra', 'dec',
                                                     'ux', 'uy', 'uz'],
                                            distinct=True,
                                            conditions=conditions)

    return_objects = [TaipanTile(c['ra'], c['dec'], field_id=c['field_id'],
                                 usposn=[c['ux'], c['uy'], c['uz']])
                      for c in centroids_db]

    logging.info('Extracted %d centroids from database' % centroids_db.shape[0])
    return return_objects
