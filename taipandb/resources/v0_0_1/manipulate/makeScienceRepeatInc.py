# Increment the visit count for passed targets
# (i.e. target was observed, but not well enought to be counted as a completion

import logging
from ....scripts.manipulate import increment_rows, update_rows

import datetime


def execute(cursor, target_ids, set_done=True,
            done_at=datetime.datetime.now(),
            inc=1):
    """
    Increment the repeat number of the passed targets.

    Parameters
    ----------
    cursor: :obj:`psycopg2.connection.cursor`
        psycopg2 cursor for communicating with the database.
    target_ids: :obj:`list` of :obj:`int`
        The list of target IDs to update the visit number for.
    set_done: :obj:`bool`
        Optional; Boolean value denoting whether to mark the target as 'done'
        or not. Defaults to True (targets will be marked as done).
    done_at: :obj:`datetime.datetime`
        Specifies the datetime at which the targets should be marked as
        complete (i.e. this value is inserted as the ``done`` value for this
        target in the database). Defaults to
        :any:`datetime.datetime.now`.

    Returns
    -------
    :obj:`None`
        Difficulties are computed, and written back to the database.
    """

    logging.info('Incrementing number of visits in database')

    # Make sure the target_ids is in list format
    target_ids = list(target_ids)

    # Increment the repeat value of the rows
    increment_rows(cursor, 'science_target', 'repeats',
                   ref_column='target_id', ref_values=target_ids, inc=inc)
    # Increment the observations value of the rows
    increment_rows(cursor, 'science_target', 'observations',
                   ref_column='target_id', ref_values=target_ids, inc=inc)
    # Set the visits value of the rows back to 0
    update_visits = list([[id, 0] for id in target_ids])
    if len(update_visits) > 0:
        update_rows(cursor, 'science_target', update_visits,
                    columns=['target_id', 'visits'])

    if set_done:
        update_done = list([[id, True] for id in target_ids])
        if len(update_done) > 0:
            update_rows(cursor, 'science_target', update_done,
                        columns=['target_id', 'success'])
            update_rows(cursor, 'science_target',
                        [(id, done_at) for id in target_ids],
                        columns=['target_id', 'done'],
                        conditions=[('t.done', 'IS', 'NULL')])

    return