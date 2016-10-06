# Collection of functions for reading Almanacs which have been
# inserted into the database
# This should be faster than using in-memory Almanacs

from src.scripts.extract import extract_from, select_min_from_joined, \
    select_max_from_joined, count_from
import taipan.scheduling as ts
import numpy as np
import copy
import datetime
import logging


def get_fields_available(cursor, datetime,
                         minimum_airmass=2.0,
                         resolution=15.):
    """
    Get the field_ids available for observation at a specified datetime.
    Parameters
    ----------
    cursor:
        psycopg2 cursor for interacting with the database.
    datetime:
        datetime.datetime denoting the UTC time that we wish to investigate.
    minimum_airmass:
        The airmass above which fields are unobservable. Defaults to 2.0.
    resolution:
        The resolution to assume the almanac data points are stored in the DB
        with. Defaults to 15 (minutes).

    Returns
    -------
    field_ids:
        A simple list of field_ids that are available at this time.
    """
    result = extract_from(cursor, 'observability',
                          columns='field_id',
                          conditions=[
                              ('airmass', '<=', minimum_airmass),
                              ('date', '<=', datetime + datetime.timedelta(
                                  minutes=resolution/2.0)),
                              ('date', '>=', datetime - datetime.timedelta(
                                  minutes=resolution/2.0)),
                          ])['field_id']
    return result


def get_airmass(cursor, field_id, dt, resolution=15.):
    """
    Get the airmass corresponding to a particular field and datetime.

    Parameters
    ----------
    cursor
    field_id
    dt

    Returns
    -------
    The airmass at that field and datetime.
    """
    result = extract_from(cursor, 'observability',
                          columns=['airmass'],
                          conditions=[
                              ('date', '>=',
                               dt - datetime.timedelta(
                                   minutes=resolution / 2.)),
                              ('date', '<=',
                               dt + datetime.timedelta(
                                   minutes=resolution / 2.)),
                              ('field_id', '=', field_id),
                          ])['airmass'][0]
    return result


def next_observable_period(cursor, field_id, datetime_from, datetime_to=None,
                           minimum_airmass=2.0, dark=True, grey=False,
                           resolution=15.):
    """
    Determine the next period when this field is observable, based on
    airmass only (i.e. doesn't consider daylight/night, dark/grey time,
    etc.).

    Uses the same inputs and returns the same outputs as using a
    taipan.scheduling.Almanac object.

    Parameters
    ----------
    datetime_from:
        Datetime which to consider from. Must be within the bounds of the
        Almanac.
    datetime_to:
        Datetime which to consider to. Defaults to None, in which case all
        available data points are used for the calculation.
    minimum_airmass:
        Minimum airmass which will allow observing. Defaults to 2.0.
    dark, grey:
        Boolean values denoting whether to return the next observable period
        of grey time, dark time, or all night time (which corresponds to both
        dark and grey being False). Trying to pass dark=True and grey=True will
        raise a ValueError.

    Returns
    -------
    obs_start, obs_end:
        The start and end times when this field is observable. Note that
        datetimes are returned in pyephem format - this is for backwards-
        compatibility with the function in ts.scheduling.Almanac objects.
    """
    # Input checking
    if datetime_to < datetime_from:
        raise ValueError('datetime_from must occur before datetime_to')
    if dark and grey:
        raise ValueError('Only one of dark or grey may be True (or both may '
                         'be False to get all night time back)')
    if datetime_to is None:
        datetime_to = select_max_from_joined(cursor, ['observability'],
                                             'date', conditions=[
                ('field_id', '=', field_id),
            ])

    # Read in the obs_start and obs_end times. It does this as follows:
    # - obs_start is the first time in the database after datetime_from and
    #   before datetime_to where the airmass is <= the limiting value
    # - obs_end is the first time in the database after obs_start and before
    #   dateimt_to where the airmass goes back above the limiting value

    conditions = [
        ('field_id', '=', field_id),
        ('date', '>=', datetime_from - datetime.timedelta(minutes=resolution)),
    ]
    if datetime_to:
        conditions += [('date', '<=', datetime_to)]
    if dark:
        conditions += [('dark', '=', True)]
    if grey:
        conditions += [('dark', '=', False)]

    # Try to get obs_start
    obs_start = select_min_from_joined(cursor, ['observability'], 'date',
                                       conditions=conditions + [
                                           ('airmass', '<=', minimum_airmass),
                                       ])

    if obs_start is None:
        # No time left, return double None
        return None, None

    conditions = [
        ('field_id', '=', field_id),
    ]
    conditions_combine = ['AND']
    if datetime_to:
        conditions += [('date', '<=', datetime_to)]
        conditions_combine += ['AND']
    if dark:
        conditions += [('(', 'dark', '=', False, '')]
    elif grey:
        conditions += [('(', 'dark', '=', True, '')]
    else:
        conditions += [('(', 'dark', 'IS', 'NULL', '')]

    obs_end = select_min_from_joined(cursor, ['observability'], 'date',
                                     conditions=conditions + [
                                         ('', 'airmass', '>',
                                          minimum_airmass, ')'),
                                         ('date', '>', obs_start)],
                                     conditions_combine=conditions_combine + [
                                         'OR', 'AND'
                                     ])

    if obs_end is None:
        # obs_end = select_max_from_joined(cursor, ['observability'], 'date',
        #                                  conditions=conditions)
        obs_end = datetime_to

    return obs_start, obs_end


def hours_observable(cursor, field_id, datetime_from, datetime_to,
                     exclude_grey_time=True,
                     exclude_dark_time=False,
                     minimum_airmass=2.0,
                     hours_better=True,
                     resolution=15.):
    """
    Calculate how many hours this field is observable for between two
    datetimes.

    This function uses (almost) the same inputs and returns the same information
    as the same function intrinsic to ts.scheduling.Almanac objects.

    Parameters
    ----------
    datetime_from, datetime_to:
        The datetimes between which we should calculate the number of
        observable hours remaining. These datetimes must be between the
        start and end dates of the almanac, or an error will be returned.
        datetime_to will default to None, such that the remainder of the
        almanac will be used.
    exclude_grey_time, exclude_dark_time:
        Boolean value denoting whether to exclude grey time or dark time
        from the calculation. Defaults to exclude_grey_time=True,
        exclude_dark_time=False (so only dark time will be counted as
        'observable'.) The legal combinations are:
        False, False (all night time is counted)
        False, True (grey time only)
        True, False (dark time only)
        Attempting to set both value to True will raise a ValueError, as
        this would result in no available observing time.
    hours_better:
        Optional Boolean, denoting whether to return only
        hours_observable which have airmasses superior to the airmass
        at datetime_now (True) or not (False). Defaults to False.

    Returns
    -------
    hours_obs:
        The number of observable hours for this field between datetime_from
        and datetime_to.

    """
    # Input checking
    # Input checking
    if datetime_to < datetime_from:
        raise ValueError('datetime_from must occur before datetime_to')
    if exclude_grey_time and exclude_dark_time:
        raise ValueError('Cannot set both exclude_grey_time and '
                         'exclude_dark_time to True - this results in no '
                         'observing time!')

    conditions = [
        ('field_id', '=', field_id),
        ('date', '>=', datetime_from),
        ('sun_alt', '<=', ts.SOLAR_HORIZON)
    ]
    if datetime_to:
        conditions += [('date', '<', datetime_to)]
    if exclude_grey_time:
        conditions += [('dark', '=', True)]
    if exclude_dark_time:
        conditions += [('dark', '=', False)]
    if hours_better:
        # Get the benchmark airmass
        minimum_airmass = min(get_airmass(cursor, field_id, datetime_from),
                              minimum_airmass)
        logging.debug('Comparison airmass: %1.3f' % minimum_airmass)
    conditions += [('airmass', '<=', minimum_airmass)]

    hours_obs = resolution * count_from(cursor, 'observability',
                                        conditions=conditions) / 60.
    return hours_obs

    # Old implementation - this code won't be reached, but is saved for the
    # moment

    # conditions = [
    #     ('field_id', '=', field_id),
    #     ('date', '>=', datetime_from),
    # ]
    # dark = False
    # grey = False
    # if datetime_to:
    #     conditions += [('date', '<=', datetime_to)]
    # if exclude_grey_time:
    #     dark = True
    # if exclude_dark_time:
    #     grey = True
    #
    # if hours_better:
    #     # Get the benchmark airmass
    #     minimum_airmass = min(get_airmass(cursor, field_id, datetime_from),
    #                           minimum_airmass)
    #     logging.debug('Comparison airmass: %1.3f' % minimum_airmass)
    #
    # hours_obs = 0.
    # dt_up_to = copy.copy(datetime_from)
    #
    # while dt_up_to < datetime_to:
    #     # Do stuff
    #     next_per_start, next_per_end = next_observable_period(cursor,
    #                                                           field_id,
    #                                                           datetime_from=
    #                                                           dt_up_to,
    #                                                           datetime_to=
    #                                                           datetime_to,
    #                                                           minimum_airmass=
    #                                                           minimum_airmass,
    #                                                           dark=dark,
    #                                                           grey=grey)
    #     if next_per_start is None:
    #         dt_up_to = copy.copy(datetime_to)
    #     else:
    #         hours_obs += ((next_per_end - next_per_start).total_seconds() /
    #                       datetime.timedelta(1.).total_seconds()) * 24.
    #         dt_up_to = next_per_end
    #
    # return hours_obs


def next_night_period(cursor, dt, limiting_dt=None,
                      dark=True, grey=False,
                      field_id=None):
    """
    Returns next period of 'night' (modulo observing conditions).

    Parameters
    ----------
    cursor:
        psycopg2 cursor for interacting with the database.
    dt:
        Datetime to consider. Should be in UTC (but naive).
    dark, grey:
        Booleans denoting whether to consider only dark or grey time. Setting
        both to False will simply return 'night' time. Setting both to True
        will raise a ValueError.

    Returns
    -------
    night_start, night_end:
        The start and end of the next 'night' period as naive datetime.datetime
        objects. These will be in UTC.
    """
    if dark and grey:
        raise ValueError('Cannot set dark and grey to both be true - only '
                         'pick one!')

    # Pick a field from the observability table - doesn't matter which one
    if field_id is None:
        fields = extract_from(cursor, 'observability', columns=['field_id'])
        field_id = fields['field_id'][0]

    conditions = [('field_id', '=', field_id)]
    if dark:
        conditions += [('dark', '=', True)]
    elif grey:
        conditions += [('dark', '=', False)]
    if limiting_dt is not None:
        conditions += [('date', '<', limiting_dt)]

    # Try to get dark_start
    dark_start = select_min_from_joined(cursor, ['observability'], 'date',
                                        conditions=conditions + [
                                            ('date', '>=', dt),
                                            ('sun_alt', '<=', ts.SOLAR_HORIZON),
                                        ])
    if dark_start is None:
        return None, None

    conditions = []
    conditions_combine = []
    if limiting_dt is not None:
        conditions += [('date', '<', limiting_dt)]
        conditions_combine += ['AND']
    if dark:
        conditions += [('(', 'dark', '=', False, '')]
        conditions_combine += ['OR']
    elif grey:
        conditions += [('(', 'dark', '=', True, '')]
        conditions_combine += ['OR']
    else:
        conditions += [('(', 'dark', 'IS', 'NULL', '')]
        conditions_combine += ['OR']

    # logging.debug(conditions)
    # logging.debug(conditions_combine)

    dark_end = select_min_from_joined(cursor, ['observability'], 'date',
                                      conditions=conditions + [
                                          ('', 'sun_alt', '>',
                                           ts.SOLAR_HORIZON, ')'),
                                          ('date', '>', dark_start),
                                      ],
                                      conditions_combine=conditions_combine + [
                                          'AND'
                                      ])
    if dark_end is None:
        # dark_end = select_max_from_joined(cursor, ['observability'], 'date')
        dark_end = limiting_dt

    return dark_start, dark_end
