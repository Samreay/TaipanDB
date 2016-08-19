# Increment the visit count for passed targets
# (i.e. target was observed, but not well enought to be counted as a completion

import logging
from ....scripts.manipulate import increment_rows, update_rows


def execute(cursor, target_ids, set_done=True):
    """
    Increment the visit number of the passed targets

    Parameters
    ----------
    cursor:
        psycopg2 cursor for communicating with the database.
    target_ids:
        The list of target IDs to update the visit number for.
    set_done:
        Optional; Boolean value denoting whether to mark the target as 'done'
        or not. Defaults to True (targets will be marked as done).

    Returns
    -------
    Nil. Difficulties are computed, and written back to the database.

    """

    logging.info('Incrementing number of visits in database')

    # Make sure the target_ids is in list format
    target_ids = list(target_ids)

    # Increment the repeat value of the rows
    increment_rows(cursor, 'science_target', 'repeats',
                   ref_column='target_id', ref_values=target_ids, inc=1)
    # Set the visits value of the rows back to 0
    update_visits = list([[id, 0] for id in target_ids])
    update_rows(cursor, 'science_target', update_visits,
                columns=['target_id', 'visits'])

    if set_done:
        update_done = list([[id, True] for id in target_ids])
        update_rows(cursor, 'science_target', update_done,
                    columns=['target_id', 'done'])

    return
