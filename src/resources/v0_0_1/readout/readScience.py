import logging
import sys
import os
import numpy as np
from ....scripts.extract import extract_from_joined, extract_from_left_joined, \
    generate_conditions_string, execute_select
from taipan.core import TaipanTarget


def execute(cursor, unobserved=False, unassigned=False, unqueued=False,
            target_ids=None, field_list=None):
    """
    Extract science targets from the database

    Parameters
    ----------
    cursor:
        psycopg2 cursor for interacting with the database
    unobserved:
        Optional; Boolean value denoting whether to only return targets
        not marked as done (True) or all targets (False). Defaults to False.
    unassigned:
        Optional; Boolean value denoting whether to only return targets
        not marked as assigned (True) or all targets (False). Defaults to False.
    unqueued:
        Optional; Boolean value denoting whether to only return targets
        not marked as queued up (True) or all targets (False). Defaults to
        False.
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
    return_objects:
        A list of TaipanTargets corresponding to the requested targets.
    """
    logging.info('Reading science targets from database')

    conditions = []
    combine = []

    if unobserved:
        conditions += [('done', 'IS', False)]
        if len(conditions) > 1:
            combine += ['AND']
    if target_ids is not None:
        conditions += [('target_id', 'IN', target_ids)]
        if len(conditions) > 1:
            combine += ['AND']
    if field_list is not None:
        if len(field_list) > 0:
            conditions += [('target_posn.field_id', 'IN', field_list)]
            if len(conditions) > 1:
                combine += ['AND']
    # if unassigned:
    #     conditions += [('(', 'is_observed', '=', True, ''),
    #                    ('', 'is_observed', 'IS', 'NULL', ')'),
    #                    ('(', 'is_queued', '=', False, ''),
    #                    ('', 'is_observed', 'IS', 'NULL', ')'),
    #                    ]
    #     if len(conditions) > 2:
    #         combine += ['AND']
    #     combine += ['OR', 'AND', 'OR']
    # if unqueued:
    #     conditions += [('(', 'is_queued', '=', False, ''),
    #                    ('', 'is_queued', 'IS', 'NULL', ')')
    #                    ]
    #     if len(conditions) > 2:
    #         combine += ['AND']
    #     combine += ['OR']

    logging.debug(conditions)
    logging.debug(combine)

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

    if not unassigned and not unqueued:
        targets_db = extract_from_left_joined(cursor, ['target',
                                                       'science_target',
                                                       'target_posn',
                                                       # 'target_field', 'tile',
                                                       ],
                                              ['target_id', 'target_id',
                                               # 'target_id',
                                               # 'tile_pk',
                                               ],
                                              conditions=conditions + [
                                                  ('is_science', "=", True,)
                                              ],
                                              conditions_combine=
                                              combine + added_conds,
                                              columns=['target_id', 'ra', 'dec',
                                                       'ux', 'uy', 'uz',
                                                       'priority', 'difficulty',
                                                       'is_queued',
                                                       'is_observed'],
                                              distinct=True)
    else:
        # For unassigned and/or unqueued, need to do a complex custom query
        # which is too hard/unique to program into a separate function
        # Thanks to Lance Luvaul (RSAA) for working this query out
        tile_conditions = []
        tile_conditions_comb = []

        if unassigned:
            tile_conditions += [
                ('(', 'is_queued', '=', False, ''),
                ('', 'is_observed', '=', False, ')')
            ]
            tile_conditions_comb += ['AND']
        if unqueued:
            tile_conditions += [('is_queued', '=', True)]
            if len(tile_conditions) > 2:
                tile_conditions_comb += ['OR']

        # Form the query - this is going to be LONG
        targets_db_raw = execute_select(
            cursor.connection(),
            "WITH foo AS ( "
            "SELECT DISTINCT target_id,ra,dec,ux,uy,uz,priority,difficulty,"
            "array_remove(array_agg(tile_pk), NULL) "
            "AS tiles FROM "
            "target LEFT JOIN science_target USING (target_id) "
            "LEFT JOIN target_posn USING (target_id) "
            "LEFT JOIN target_field USING (target_id) "
            "WHERE %s "
            "GROUP BY target_id )"
            "SELECT DISTINCT target_id,ra,dec,ux,uy,uz,priority,difficulty "
            "FROM foo WHERE NOT EXISTS ("
            "SELECT 1 FROM unnest(tiles) AS test WHERE test IN ("
            "SELECT tile_pk FROM tile WHERE (%s))"
            % (generate_conditions_string(conditions, combine),
               generate_conditions_string(tile_conditions,
                                          tile_conditions_comb))
        )

        # Form the return into a structured array
        targets_db = np.array(targets_db_raw, dtype={
            'names': ['target_id', 'ra', 'dec', 'ux', 'uy', 'uz',
                      'priority', 'difficulty'],
            'types': ['f8', 'f8', 'f8', 'f8', 'f8', 'f8', 'i4', 'i4']
        })



    logging.debug('Forming return TaipanTarget objects')
    return_objects = [TaipanTarget(
        g['target_id'], g['ra'], g['dec'], priority=g['priority'],
        difficulty=g['difficulty'],
        ucposn=(g['ux'], g['uy'], g['uz']),
        ) for g in targets_db]

    logging.info('Extracted %d targets from database' % len(return_objects))
    return return_objects
