# Compute the number of each type of targets remaining in each field
# Note this does a full, in-bulk calculation - for smaller changes, it is
# probably more efficient to do direct manipulation

import logging
import numpy as np
from taipan.core import TaipanTarget
from ....scripts.extract import extract_from_joined
from ....scripts.manipulate import update_rows


def execute(cursor):
    """
    Calculate the number of targets in each field of each status type.

    Parameters
    ----------
    cursor:
        psycopg2 cursor for communicating with the database.

    Returns
    -------
    Nil - the database is updated in place. The function looks at the
    contents of the science_target table, and places targets into three
    categories:
    - Remaining (i.e observing is not complete)
    - Allocated (i.e. assigned to a tile, but observing isn't complete)
    - Observed (i.e. observing that target is done)
    It then writes these values to the relevant columns (n_sci_rem, n_sci_alloc,
    n_sci_obs) of the tiling_info table.
    """

    logging.info('Doing a bulk calculation of per-field target statuses')

    # Unfortunately, we can't just use the assigned tile/field to increment
    # the target count for that tile/field - targets can and will appear in
    # multiple fields

    # Read completed targets
    logging.debug('Extracting observed targets...')
    targets_stats_array = extract_from_joined(cursor,
                                              ['target', 'science_target'],
                                              conditions=[
                                                  ('is_science', "=", True),
                                                  ('done', "=", True),
                                              ],
                                              columns=['target_id', 'ra', 'dec',
                                                       'ux', 'uy', 'uz'])
    logging.debug('Extracted %d observed targets' % len(targets_stats_array))

    # Read targets that are assigned, but yet to be observed
    # Targets must fulfil two criteria:
    # - They can't be marked as done in science_target
    # - The assigned tile must not be marked observed
    logging.debug('Extracting assigned targets...')
    target_stats_array = extract_from_joined(cursor,
                                             ['target', 'science_target',
                                              'target_field', 'tile', 'field'],
                                             conditions=[
                                                 ('is_science', '=', True),
                                                 ('done', '=', False),
                                                 ('is_observed', '=', False),
                                             ],
                                             columns=['target_id', 'ra', 'dec',
                                                      'ux', 'uy', 'uz'])
    logging.debug('Extracted %d assigned targets' % len(targets_stats_array))

    # Read targets which are not assigned to any tile yet, nor observed
    # Note that this means we have to find any targets which either:
    # - Have no entries in target_field;
    # - Have entries in target_field, but all the related tiles are set to
    #   'observed' and the target isn't marked as 'done'
    logging.debug('Extracting currently unassigned targets...')
    target_stats_array_a = extract_from_joined(cursor,
                                               ['target', 'science_target',
                                                'target_field', 'tile',
                                                'field'],
                                               conditions=[
                                                   ('is_science', '=', True),
                                                   ('done', '=', False),
                                                   ('is_observed', '=', True),
                                               ],
                                               columns=['target_id', 'ra', 'dec',
                                                        'ux', 'uy', 'uz'])
    target_stats_array_b = extract_from_joined(cursor,
                                               ['target', 'science_target',
                                                'target_field'],
                                               conditions=[
                                                   ('is_science', '=', True),
                                                   ('done', '=', False),
                                                   ('tile_pk', 'IS', 'NULL'),
                                               ],
                                               columns=['target_id', 'ra',
                                                        'dec',
                                                        'ux', 'uy', 'uz'])
    target_stats_array = np.concatenate((target_stats_array_a,
                                         target_stats_array_b, ))
    logging.debug('Extracted %d assigned targets (%d from no assignments, '
                  '%d from completed assignments but target incomplete' %
                  (len(target_stats_array), len(target_stats_array_a),
                   len(target_stats_array_b), ))

    return

