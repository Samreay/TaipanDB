# Compute the number of each type of targets remaining in each field
# Note this does a full, in-bulk calculation - for smaller changes, it is
# probably more efficient to do direct manipulation

import logging
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

    return


    targets_stats_array = extract_from_joined(cursor,
                                              ['target', 'science_target',
                                               'tile', 'target_field'],
                                              conditions=[
                                                  ('is_science', "=", True),
                                              ],
                                              columns=['target_id', 'ra', 'dec',
                                                       'ux', 'uy', 'uz',
                                                       'done', 'tile_pk'])
    logging.debug('Extracted %d targets' % len(targets_stats_array))

    return