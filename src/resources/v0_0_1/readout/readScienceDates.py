import logging
import datetime
import numpy as np

from src.scripts.extract import extract_from_left_joined

def execute(cursor, tgt_list=None, repeats=1):
    """
    Find out when a target was a) first observed and b) completed.

    The logic for this function is somewhat convoluted, as targets may be
    unobserved, partially observed, have a fully completed repeat, have a
    fully completed repeat and be halfway through the next repeat, etc.
    Therefore, this function will report the following:
    - An unobserved target will have no started or completed dates.
    - A target part-way through its first repeat will have a started date,
    but no end date.
    - A target with the first repeat complete will have a started date and a
    completed date.
    - A target with at least one repeat complete, and subsequent repeats either
    underway or completed, will report a start date and completed date relevant
    to the first repeat only, *unless* the repeats keyword argument is passed
    and is greater than one (see description of that argument below). In this
    case, the start and end date will be returned relative to the currently-
    underway repeat, or the last repeat completed if the next repeat has
    not been activated.


    Parameters
    ----------
    cursor:
        A psycopg2 cursor for interacting with the database.
    tgt_list:
        Optional; a list of target_ids to query. Defaults to None, at which
        point all (science) targets will be queried. WARNING: passing a large
        number of target_ids will result in very slow execution.
    repeats:
        Optional; integer number of repeats to consider before simply assuming
        a target is 'done'. If left on the default value of 1, a target will
        always report a start date and completed date relative to the first
        repeat, regardless of whether other repeats have been activated and/or
        completed within the system. If set to n > 1, then start and
        completion dates will be reported as follows:
        - If repeat m < n has been completed, and no further repeat has been
        activated (i.e. done=True for the target), the start and completed
        dates will be reported for the most recent finished repeat.
        - If repeat m < n has been completed, and a new repeat has been
        activated (done=False) but not yet started, the target will report
        None for both the start and completed dates.
        - If repeat m <= n is currently underway, start date will be reported
        for the target, and completed date will be None;
        - If repeat n has been completed, start and completed dates will be
        reported for that repeat will be reported without consideration for
        any further repeats.

    Returns
    -------
    target_dates:
        A NumPy structured array, where each element takes the form
        (target_id, start_date, completed_date).
        target_id is an integer - dates are datetime.datetime objects.
    """
    # Input checking
    repeats = int(repeats)
    if repeats < 1:
        raise ValueError('repeats must be >= 1 (default 1)')
    tgt_list = list(tgt_list)

    logging.info('Reading/computing target started/completed times')

    conditions = []
    if tgt_list:
        conditions += [('target_id', 'IN', tgt_list)]

    # Easy ones first - let's find all the targets that have no observations
    # against them
    tgt_not_started = extract_from_left_joined(cursor, ['science_target',
                                                        'target_field',
                                                        'tile'],
                                               ['target_id', 'tile_pk'],
                                               conditions=conditions + [
                                                   ('done', '=', False),
                                                   ('date_obs', 'IS', 'NULL'),
                                               ], columns=['target_id'])
