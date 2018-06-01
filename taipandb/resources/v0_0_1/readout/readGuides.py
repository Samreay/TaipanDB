import logging
import sys
import os
from ....scripts.extract import extract_from_left_joined
from taipan.core import TaipanTarget


def execute(cursor, field_list=None, active_only=True):
    """
    Read guide targets from the database.

    Parameters
    ----------
    cursor: :obj:`psycopg2.connection.cursor`
        psycopg2 cursor for interacting with the database.
    field_list: :obj:`list` of :obj:`int`
        Optional, list of field IDs to compare guides against. Guide targets
        will only be returned if they appear in one of the fields specified.
        Defaults to None, at which point all guides will be returned.

    Returns
    -------
    return_objects: :obj:`list` of :obj:`taipan.core.TaipanTarget`
        A list of TaipanTarget objects corresponding to the guide targets in
        the database.
    """
    logging.info('Reading guides from database')

    if field_list is not None:
        if len(field_list) == 0:
            field_list = None

    conditions = [('is_guide', "=", True)]
    if field_list:
        conditions += [('field_id', 'IN', field_list)]
    if active_only:
        conditions += [('is_active', '=', True)]

    guides_db = extract_from_left_joined(cursor, ['target', 'target_posn', ],
                                         'target_id',
                                         conditions=conditions,
                                         columns=['target_id', 'ra', 'dec',
                                                  'mag',
                                                  'ux', 'uy', 'uz'],
                                         distinct=True)

    return_objects = [TaipanTarget(
        g['target_id'], g['ra'], g['dec'], guide=True, science=False,
        usposn=(g['ux'], g['uy'], g['uz']), mag=g['mag'],
        ) for g in guides_db]

    logging.info('Extracted %d guides from database' % guides_db.shape[0])
    return return_objects
