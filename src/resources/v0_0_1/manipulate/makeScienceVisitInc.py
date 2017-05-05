# Increment the visit count for passed targets
# (i.e. target was observed, but not well enought to be counted as a completion

import logging
from ....scripts.manipulate import increment_rows


def execute(cursor, target_ids, inc=1):
    """
    Increment the visit number of the passed targets

    Parameters
    ----------
    cursor:
        psycopg2 cursor for communicating with the database.
    target_ids:
        The list of target IDs to update the visit number for.
    inc:
        The increment value. Defaults to 1.

    Returns
    -------
    Nil. Difficulties are computed, and written back to the database.

    """

    logging.info('Incrementing number of visits in database')

    # Make sure the target_ids is in list format
    target_ids = list(target_ids)

    # Increment the rows
    if len(target_ids) > 0:
        increment_rows(cursor, 'science_target', 'visits',
                       ref_column='target_id', ref_values=target_ids, inc=inc)
        # Increment the observations value of the rows
        increment_rows(cursor, 'science_target', 'observations',
                       ref_column='target_id', ref_values=target_ids, inc=inc)

    return
