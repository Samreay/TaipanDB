import logging
import sys
import os
from ....scripts.extract import extract_from_left_joined
from taipan.core import TaipanTarget


def execute(cursor, field_list=None):
    """
    Read standard targets from the database.

    Parameters
    ----------
    cursor:
        psycopg2 cursor for interacting with the database.
    field_list:
        Optional, list of field IDs to compare guides against. Guide targets
        will only be returned if they appear in one of the fields specified.
        Defaults to None, at which point all guides will be returned.

    Returns
    -------
    return_objects:
        List of TaipanTarget objects corresponding to the standards listed in
        the database.
    """
    logging.info('Reading standards from database')

    if field_list is not None:
        if len(field_list) == 0:
            field_list = None

    conditions = [('is_standard', "=", True)]
    if field_list:
        conditions += [('field_id', 'IN', field_list)]

    standards_db = extract_from_left_joined(cursor, ['target', 'target_posn'],
                                            'target_id',
                                            conditions=conditions,
                                            columns=['target_id', 'ra', 'dec',
                                                     'ux', 'uy', 'uz'],
                                            distinct=True)

    return_objects = [TaipanTarget(s['target_id'], s['ra'], s['dec'],
                                   standard=True,
                                   ucposn=(s['ux'], s['uy'],
                                           s['uz'])) for s in standards_db]

    logging.info('Extracted %d standards from database' % standards_db.shape[0])
    return return_objects
