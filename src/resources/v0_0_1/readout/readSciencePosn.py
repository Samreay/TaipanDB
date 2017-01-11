import logging
import sys
import os
from ....scripts.extract import extract_from_joined
from taipan.core import TaipanTarget
from matplotlib.cbook import flatten


def execute(cursor, field_list=None, target_list=None):
    """
    Retrieve an array of target IDs and associated fields.

    Parameters
    ----------
    cursor:
        psycopg2 cursor for communicating with the database
    field_list: list of ints, optional
        List of fields to consider. Defaults to None, at which point
        information fo all fields will be returned. WARNING: Using a large
        field_list will seriously affect performance!
    target_list: list of ints, optional
        List of target_ids to consider. Defaults to None, at which point
        information fo all targets will be returned. WARNING: Using a large
        field_list will seriously affect performance!

    Returns
    -------
    db_return: np.array
        A numpy structured array, with elements ['target_id', 'field_id'].
    """
    logging.info('Reading science positions from database')

    # Input checking

    conditions = []
    if field_list:
        conditions += [('field_id', 'IN', field_list)]
    if target_list:
        conditions += [('target_id', 'IN', target_list)]

    db_return = extract_from_joined(cursor, ['target_posn'],
                                    conditions=conditions,
                                    columns=['target_id', 'field_id'])

    logging.info('Extracted %d target positions from database' % len(db_return))
    return db_return
