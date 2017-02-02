# Increment the visit count for passed targets
# (i.e. target was observed, but not well enought to be counted as a completion

import logging
from ....scripts.manipulate import increment_rows, update_rows


def execute(cursor, target_ids, priorities):
    """
    Update target priorities in the database

    Parameters
    ----------
    cursor:
        psycopg2 cursor for communicating with the database.
    target_ids:
        The list of target_ids to do priority updates for.
    priorities:
        List of prioritites to assign. This must be the same length as
        target_ids, and have a one-to-one correspondence with that list.

    Returns
    -------
    Nil. Priorities are written into the database

    """

    logging.info('Writing priorities to database')

    # Make sure the target_ids is in list format
    target_ids = list(target_ids)
    priorities = list(priorities)

    if len(target_ids) == 0:
        return
    if len(target_ids) != len(priorities):
        raise ValueError('target_ids and priorities must be of the same '
                         'length!')

    update_array = [[target_ids[i], priorities[i]] for
                    i in range(len(target_ids))]

    update_rows(cursor, 'science_target', update_array,
                columns=['target_id', 'priority'])

    return
