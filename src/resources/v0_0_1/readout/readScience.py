import logging
import sys
import os
from ....scripts.extract import extract_from_joined
from taipan.core import TaipanTarget


def execute(cursor, unobserved=False, target_ids=None):
    """
    Extract science targets from the database

    Parameters
    ----------
    cursor:
        psycopg2 cursor for interacting with the database
    unobserved:
        Optional; Boolean value denoting whether to only return targets
        not marked as done (True) or all targets (False). Defaults to False.
    target_ids:
        Optional; list of target_ids corresponding to the targets to extract
        from the database. Defaults to None, at which point all targets present
        will be extracted.

    Returns
    -------
    return_objects:
        A list of TaipanTargets corresponding to the requested targets.
    """
    logging.info('Reading science targets from database')

    conditions = []

    if unobserved:
        conditions += [('done', 'IS', False)]
    if target_ids is not None:
        conditions += [('target_id', 'IN', target_ids)]

    targets_db = extract_from_joined(cursor, ['target', 'science_target'],
                                     conditions=conditions + [
                                         ('is_science', "=", True, )
                                     ],
                                     columns=['target_id', 'ra', 'dec',
                                              'ux', 'uy', 'uz', 'priority',
                                              'difficulty'])

    return_objects = [TaipanTarget(
        g['target_id'], g['ra'], g['dec'], priority=g['priority'],
        difficulty=g['difficulty'],
        ucposn=(g['ux'], g['uy'], g['uz']),
        ) for g in targets_db]

    logging.info('Extracted %d targets from database' % len(return_objects))
    return return_objects
