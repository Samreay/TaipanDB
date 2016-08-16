import logging
import sys
import os
from ....scripts.extract import extract_from
from taipan.core import TaipanTarget


def execute(cursor, target_ids=None):
    """
    Retrieve an array of target IDs and associated types.

    Parameters
    ----------
    cursor:
        psycopg2 cursor for communicating with the database
    target_ids:
        Optional list of target_ids to return. Defaults to None, at which point
        all targets in the database are returned.

    Returns
    -------
    targets_db:
        A numpy structured array of data from the science_target table in the
        database. Each row is of the format [target_id, is_vpec_target,
        is_HO_target, is_lowz_target].

    """
    logging.info('Reading science targets (types) from database')

    if target_ids is not None:
        conditions = [('target_id', 'IN', tuple(target_ids)), ]
    else:
        conditions = None

    targets_db = extract_from(cursor, 'science_target',
                              conditions=conditions,
                              columns=['target_id', 'is_vpec_target',
                                       'is_H0_target', 'is_lowz_target'])

    logging.info('Extracted %d targets from database' % len(targets_db))
    return targets_db
