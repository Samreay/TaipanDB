import logging
import sys
import os
from ....scripts.extract import extract_from
from taipan.core import TaipanTarget

def execute(cursor):
    """
    Read standard targets from the database.

    Parameters
    ----------
    cursor:
        psycopg2 cursor for interacting with the database.

    Returns
    -------
    return_objects:
        List of TaipanTarget objects corresponding to the standards listed in
        the database.
    """
    logging.info('Reading standards from database')

    standards_db = extract_from(cursor, 'target',
                                conditions=[('is_standard', "=", True)],
                                columns=['target_id', 'ra', 'dec',
                                         'ux', 'uy', 'uz'])

    return_objects = [TaipanTarget(s['target_id'], s['ra'], s['dec'],
                                   standard=True,
                                   ucposn=(s['ux'], s['uy'],
                                           s['uz'])) for s in standards_db]

    logging.info('Extracted %d standards from database' % standards_db.shape[0])
    return return_objects
