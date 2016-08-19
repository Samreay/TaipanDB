import logging
import sys
import os
from ....scripts.extract import extract_from
from taipan.core import TaipanTarget

def execute(cursor):
    """
    Read guide targets from the database.

    Parameters
    ----------
    cursor:
        psycopg2 cursor for interacting with the database.

    Returns
    -------
    return_objects:
        A list of TaipanTarget objects corresponding to the guide targets in
        the database.
    """
    logging.info('Reading guides from database')

    guides_db = extract_from(cursor, 'target',
                             conditions=[('is_guide', "=", True)],
                             columns=['target_id', 'ra', 'dec',
                                      'ux', 'uy', 'uz'])

    return_objects = [TaipanTarget(
        g['target_id'], g['ra'], g['dec'], guide=True,
        ucposn=(g['ux'], g['uy'], g['uz']),
        ) for g in guides_db]

    logging.info('Extracted %d guides from database' % guides_db.shape[0])
    return return_objects
