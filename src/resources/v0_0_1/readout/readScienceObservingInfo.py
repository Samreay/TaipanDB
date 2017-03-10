import logging
import sys
import os
import numpy as np
from ....scripts.extract import extract_from_joined, extract_from_left_joined, \
    select_group_agg_from_joined
from taipan.core import TaipanTarget


def execute(cursor, target_ids=None, field_list=None):
    """
    Extract the observing status of science targets from the database.

    Parameters
    ----------
    cursor:
        psycopg2 cursor for interacting with the database.
    target_ids:
        Optional; list of target_ids corresponding to the targets to extract
        from the database. Defaults to None, at which point all targets present
        will be extracted. WARNING: Providing a large list of target_ids will
        make the database query very slow!
    field_list:
        Optional; list of field IDs for which targets should be returned.
        Membership of fields is determined by joining against the target_posn
        database table. Note that, if used in conjunction with target_ids, only
        targets satisfying *both* criteria will be returned.

    Returns
    -------
    target_info, targets_tiles, targets_completed:
        Numpy structured arrays that contain the following information:
        - target_info: Basic target info, including type, visit/repeat, position
        - target_tiles: Listing of tile_pks against target_ids with obs date
        - targets_completed: The dates at which completed targets were last seen
    """
    logging.info('Reading science target observing info from database')

    conditions = []

    if target_ids is not None:
        conditions += [('target_id', 'IN', target_ids)]
    if field_list is not None:
        if len(field_list) > 0:
            conditions += [('target_posn.field_id', 'IN', field_list)]

    logging.debug(conditions)

    # Old query (no ability to do unassigned, unqueued)
    # targets_db = extract_from_joined(cursor, ['target', 'science_target'],
    #                                  conditions=conditions + [
    #                                      ('is_science', "=", True, )
    #                                  ],
    #                                  columns=['target_id', 'ra', 'dec',
    #                                           'ux', 'uy', 'uz', 'priority',
    #                                           'difficulty'])

    if len(conditions) > 0:
        added_conds = ['AND']
    else:
        added_conds = []

    targets_db = extract_from_joined(cursor, ['target', 'science_target'],
                                     columns=['target_id', 'ra', 'dec',
                                              'is_h0_target', 'is_vpec_target',
                                              'is_lowz_target', 'visits',
                                              'repeats', 'done'],
                                     conditions=conditions)

    # Need to separately determine when each target was completed (if it was)

    # Now we need to fetch a structured array showing which targets were on
    # which tiles
    targets_tiles = extract_from_joined(cursor, ['target_field',
                                                 'target', 'tile',
                                                 'tiling_config'],
                                        # ['target_id', 'tile_pk', ],
                                        columns=['target_id', 'tile_pk',
                                                 'date_obs'],
                                        conditions=[
                                            ('is_observed', '=', True),
                                            ('date_obs', 'IS NOT', 'NULL'),
                                            ('target_id', '!=', -1),
                                            ('is_science', '=', True)
                                        ])

    # Get the completion date for each completed target
    targets_completed = select_group_agg_from_joined(cursor,
                                                     ['target_field',
                                                      'tiling_config',
                                                      'science_target',
                                                      'target'],
                                                     'max', 'date_obs',
                                                     'target_id',
                                                     conditions=[
                                                         ('is_science', '=',
                                                          True),
                                                         ('done', 'IS NOT',
                                                          'NULL'),
                                                         ('date_obs', 'IS NOT',
                                                          'NULL')
                                                     ])

    return targets_db, targets_tiles, targets_completed
