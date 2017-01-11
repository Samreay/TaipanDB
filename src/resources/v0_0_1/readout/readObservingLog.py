import logging
import sys
import os
import datetime
import numpy as np
from ....scripts.extract import extract_from_joined, extract_from_left_joined, \
    select_group_agg_from_joined
from taipan.core import TaipanTarget
from taipan.scheduling import utc_local_dt


def execute(cursor, target_ids=None, field_list=None,
            date_start=None, date_end=None):
    """
    Generate an observing log from the database.

    Parameters
    ----------
    cursor:
        psycopg2 cursor for interacting with the database.
    target_ids:
        Optional; list of target_ids corresponding to the targets to extract
        from the database. Defaults to None, at which point all targets present
        will be extracted. WARNING: Providing a large list of target_ids will
        make the database query very slow!
    field_list:
        Optional; list of field IDs for which targets should be returned.
        Membership of fields is determined by joining against the target_posn
        database table. Note that, if used in conjunction with target_ids, only
        targets satisfying *both* criteria will be returned.
    date_start, date_end: datetime.date, optional
        Start and end dates of the period to be extracted from the database.
        Both default to None, at which point all data points from the database
        will be returned.


    Returns
    -------
    obs_log: numpy structured array
        Numpy structured arrays that contains one row for each time a science
        target has been observed.
    """
    logging.info('Generating observing log from database')

    if date_start is not None and date_end is not None:
        if date_end < date_start:
            raise ValueError('date_end must be after date_start!')

    midday = datetime.time(12, 0, 0)

    conditions = [
        ('is_science', '=', True),
        ('date_obs', 'IS NOT', 'NULL'),
    ]

    if target_ids is not None:
        conditions += [('target_id', 'IN', target_ids)]
    if field_list is not None:
        if len(field_list) > 0:
            conditions += [('field_id', 'IN', field_list)]
    if date_start:
        conditions += [
            ('date_obs', '>=',
             utc_local_dt(datetime.datetime.combine(date_start, midday)))
        ]
    if date_end:
        conditions += [
            ('date_obs', '<=',
             utc_local_dt(datetime.datetime.combine(date_end, midday)) +
             datetime.timedelta(1))
        ]

    logging.debug(conditions)

    obs_log = extract_from_joined(cursor,
                                  ['tiling_config', 'tile', 'target_field',
                                   'science_target', 'target'],
                                  columns=['target_id', 'ra', 'dec', 'tile_pk',
                                           'is_h0_target', 'is_lowz_target',
                                           'is_vpec_target', 'priority',
                                           'date_obs', 'done'],
                                  conditions=conditions)

    return obs_log
