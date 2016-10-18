# Compute the difficulties of non-satisfied targets in the database

import logging
from taipan.core import compute_target_difficulties
from taipan.core import TaipanTarget
from ....scripts.extract import extract_from_joined
from ....scripts.manipulate import update_rows


def execute(cursor, use_only_notdone=True,
            priority_cut=True):
    """
    Compute the difficulties of TaipanTargets and write them back to the
    database.

    Parameters
    ----------
    cursor:
        psycopg2 cursor for communicating with the database.
    use_only_notdone:
        Optional Boolean, denoting whether to only use un-done targets in
        the calculation (True), or not (False). Defaults to True.
    priority_cut:
        Optional Boolean, denoting whether each target's difficulty should
        be calculated using only targets with the same or lower priority
        (True), or all targets (False). Defaults to True.

    Returns
    -------
    Nil. Difficulties are computed, and written back to the database.

    """

    logging.info('Reading science targets from database')

    if use_only_notdone:
        conditions = [("done", "=", False)]
    else:
        conditions = None

    targets_db = extract_from_joined(cursor, ['target', 'science_target'],
                                     conditions=conditions,
                                     columns=['target_id', 'ra', 'dec',
                                              'ux', 'uy', 'uz', 'priority'])

    return_objects = [TaipanTarget(
        g['target_id'], g['ra'], g['dec'], priority=g['priority'],
        ucposn=(g['ux'], g['uy'], g['uz']),
    ) for g in targets_db]

    logging.info('Extracted %d targets from database' % len(return_objects))

    # Do the difficulty computation
    logging.info('Computing difficulties...')
    if priority_cut:
        # Rather than do a per-target calculation, do the efficient
        # multi-target calculation for each priority, including all lower
        # priorities. This will create a cascade effect that will leave all
        # targets with the correct priorities.
        priorities = list(set(targets_db['priority']))
        priorities.sort()
        for p in priorities[::-1]:
            logging.debug('Computing difficulties for priority %d targets' %
                          p)
            compute_target_difficulties([o for o in return_objects if
                                         o.priority <= p])
    else:
        compute_target_difficulties(return_objects)

    # Construct a data array to write back to the database
    data = [(t.idn, t.difficulty) for t in return_objects]

    # Write the results to the science_target table
    update_rows(cursor, 'science_target', data,
                columns=['target_id', 'difficulty'])
    logging.info('Wrote back %d rows of science_target' % len(data))

    return return_objects
