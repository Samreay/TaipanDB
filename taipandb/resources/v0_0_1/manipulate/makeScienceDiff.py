# Compute the difficulties of non-satisfied targets in the database

import logging
from taipan.core import compute_target_difficulties, TaipanTarget
from taipan.core import TARGET_PRIORITY_MS
from ....scripts.extract import extract_from, extract_from_joined
from ....scripts.manipulate import update_rows, update_rows_temptable

from ..readout import readCentroidsAffected as rCA

import numpy as np


def execute(cursor, use_only_notdone=True,
            priority_cut=True, target_list=None,
            active_only=True, batch_size=50000):
    """
    Compute and storethe difficulties of :any:`taipan.core.TaipanTarget`.

    Parameters
    ----------
    cursor: :obj:`psycopg2.connection.cursor`
        psycopg2 cursor for communicating with the database.
    use_only_notdone: :obj:`bool`
        Optional Boolean, denoting whether to only use un-done targets in
        the calculation (True), or not (False). Defaults to True.
    priority_cut: :obj:`bool`
        Optional Boolean, denoting whether each target's difficulty should
        be calculated using only targets with the same or lower priority
        (True), or all targets (False). Defaults to True.
    target_list : :obj:`list` of :obj:`int`
        Optional list of target IDs for which we wish to compute
        difficulties.

    Returns
    -------
    :obj:`None`
        Difficulties are computed, and written back to the database.
    """

    logging.info('Computing difficulties for science targets')

    if use_only_notdone:
        conditions = [("done", "IS", "NULL")]
    else:
        conditions = []

    conditions += [('is_science', '=', True)]

    if target_list is not None:
        target_list = list(target_list)

        # Work out which fields these targets are on
        target_fields = extract_from(cursor, 'target_posn',
                                     conditions=[('target_id', 'IN',
                                                  target_list), ],
                                     columns=['target_id',
                                              'field_id'])['field_id']

        # Now work out which fields are affected by this
        affected_fields = rCA.execute(cursor, field_list=target_fields)
        # Or, to increases speed, only re-compute difficulties for target_fields
        # This is wrong, but not grossly so
        # affected_fields = target_fields

        conditions += [('field_id', 'IN', affected_fields)]

    if active_only:
        conditions += [('is_active', '=', True)]

    # We need to read in *all* the targets, not just the ones we want to
    # re-compute difficulties for
    targets_db = extract_from_joined(cursor, ['target', 'science_target',
                                              'target_posn'],
                                     conditions=conditions,
                                     columns=['target_id', 'ra', 'dec',
                                              'ux', 'uy', 'uz', 'priority'],
                                     distinct=True)

    return_objects = [TaipanTarget(
        g['target_id'], g['ra'], g['dec'], priority=g['priority'],
        usposn=(g['ux'], g['uy'], g['uz']),
    ) for g in targets_db]

    logging.info('Extracted %d targets from database' % len(return_objects))

    # Do the difficulty computation
    logging.info('Computing difficulties...')
    for i in range(0, len(return_objects), batch_size):
        sublist = return_objects[i:i+batch_size]
        if priority_cut:
            # Rather than do a per-target calculation, do the efficient
            # multi-target calculation for each priority, including all lower
            # priorities. This will create a cascade effect that will leave all
            # targets with the correct priorities.
            # if target_list is not None:
            #     priorities = list(set(targets_db[
            #                               np.in1d(targets_db['target_id'],
            #                                       target_list)]['priority']))
            # else:
            #     priorities = list(set(targets_db['priority']))
            # priorities.sort()
            #
            # for p in priorities[::-1]:
            #     logging.debug('Computing difficulties for priority %d targets' %
            #                   p)
            #     if target_list is not None:
            #         compute_target_difficulties([o for o in return_objects if
            #                                      o.priority == p and
            #                                      o.idn in target_list],
            #                                     full_target_list=[o for o in
            #                                                       return_objects if
            #                                                       o.priority <= p])
            #     else:
            #         compute_target_difficulties([o for o in return_objects if
            #                                      o.priority <= p])

            # Update 17-02-24: Compute difficulties in two tranches:
            # >= TARGET_PRIORITY_MS and < TARGET_PRIORITY_MS
            if target_list is not None:
                compute_target_difficulties([o for o in sublist if
                                             o.idn in target_list and
                                             o.priority < TARGET_PRIORITY_MS],
                                            full_target_list=return_objects)
                compute_target_difficulties([o for o in sublist if
                                             o.idn in target_list and
                                             o.priority >= TARGET_PRIORITY_MS],
                                            full_target_list=[o for o in
                                                              return_objects if
                                                              o.priority >=
                                                              TARGET_PRIORITY_MS])
            else:
                compute_target_difficulties([o for o in sublist if
                                             o.priority < TARGET_PRIORITY_MS],
                                            full_target_list=return_objects)
                compute_target_difficulties([o for o in sublist if
                                             o.priority >= TARGET_PRIORITY_MS],
                                            full_target_list=[o for o in
                                                              return_objects if
                                                              o.priority >=
                                                              TARGET_PRIORITY_MS])
        else:
            if target_list is not None:
                compute_target_difficulties(
                    [o for o in sublist if o.idn in target_list],
                    full_target_list=return_objects)
            else:
                compute_target_difficulties(return_objects)


    # Construct a data array to write back to the database
    if target_list is not None:
        data = [(t.idn, t.difficulty) for t in return_objects if
                t.idn in target_list]
    else:
        data = [(t.idn, t.difficulty) for t in return_objects]

    # Write the results to the science_target table
    update_rows_temptable(cursor, 'science_target', data,
                          columns=['target_id', 'difficulty'])
    logging.info('Wrote back %d rows of science_target' % len(data))

    return return_objects
