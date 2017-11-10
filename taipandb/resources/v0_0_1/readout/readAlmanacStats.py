"""
Read various Almanac (i.e. observability) statistics

This sub-module differs from most other in :any:`src.resources` by containing
multiple public methods (whereas most other submodules define a single
`execute` method).
"""

from taipandb.scripts.extract import extract_from, select_min_from_joined, \
    select_max_from_joined, count_from, select_agg_from_joined, \
    count_grouped_from_joined, extract_from_joined, select_having
import taipan.scheduling as ts
import numpy as np
import copy
import datetime
import logging

DT_RANGE_FUDGE = datetime.timedelta(seconds=10.)
"""
This is padding added to start and end of a searched-for date range - it
prevents floating point errors causing the system to report, e.g., that
nothing is available for observation
"""


def check_almanac_finish(cursor):
    """
    Find the last date which the almanacs are calibrated for.

    Parameters
    ----------
    cursor : :obj:`psycopg2.connection.cursor`
        For communication with the database

    Returns
    -------
    datetime_max : :obj:`datetime.datetime`
        The final date found in the observability database table
    """
    datetime_max = select_max_from_joined(cursor,
                                          ['observability'], 'date')
    return datetime_max


def get_fields_available_pointing(cursor, dt,
                                  minimum_airmass=2.0,
                                  resolution=15.,
                                  pointing_time=ts.POINTING_TIME,
                                  active_only=True):
    """
    Get the fields available for observation from now until the end of a
    complete pointing time

    This function does not consider dark/grey time, but simply whether a
    field is above the `minimum_airmass` or not.

    Parameters
    ----------
    cursor : :obj:`psycopg2.connection.cursor`
        For communication with the observing database
    dt : :obj:`datetime.datetime`, UTC
        Time of interest
    minimum_airmass : :obj:`float`
        Airmass above which the field centre must be to be considered
        observable. Defaults to 2.0.
    resolution : :obj:`float`, minutes
        The resolution of the Almanacs in the database. Defaults to 15.
    pointing_time : :obj:`float`, days
        The length of a complete pointing (slew, reconfigure, observe) in
        days. Defaults to :any:`taipan.scheduling.POINTING_TIME`.

    Returns
    -------
    field_ids : :obj:`np.array` of :obj:`int`
        Array of field IDs that are currently available to observe
    """

    # Run the query
    result = select_having(cursor, 'observability', 'field_id',
                           conditions=[
                               ('date', '>=', dt -
                                datetime.timedelta(minutes=resolution/2.) -
                                DT_RANGE_FUDGE),
                               ('date', '<=', dt +
                                datetime.timedelta(minutes=resolution / 2.) +
                                datetime.timedelta(days=pointing_time) +
                                DT_RANGE_FUDGE),
                           ],
                           having=[('airmass', '<=', minimum_airmass)])

    # Strip the result down to a singular list
    result = [_[0] for _ in result]

    if active_only:
        result = extract_from(cursor, 'field',
                              conditions=[
                                  ('field_id', 'IN', result,),
                              ],
                              columns=['field_id'])

    return result


def next_time_available(cursor, dt, end_dt=None,
                        minimum_airmass=2.0,
                        resolution=15.):
    """
    Return the next time that at least one field will be available to observe

    Parameters
    ----------
    cursor : :obj:`psycopg2.connection.cursor`
        For communication with the observing database
    dt : :obj:`datetime.datetime`, UTC
        Time of interest
    end_dt: :obj:`datetime.datetime`, UTC
        The end of the observing period that we are interested in. Defaults
        to None, in which case this criterion will not be applied
    minimum_airmass : :obj:`float`
        Airmass above which the field centre must be to be considered
        observable. Defaults to 2.0.
    resolution : :obj:`float`, minutes
        The resolution of the Almanacs in the database. Defaults to 15.

    Returns
    -------
    next_time : :obj:`datetime.datetime`, UTC, OR :obj:`None`
        The next time that a field will be available for observation. If no
        fields will be available before ``end_dt``, :obj:`None` will be
        returned.
    """

    conditions = [
        ('date', '>', dt),
        ('airmass', '<=',
         minimum_airmass),
    ]
    if end_dt:
        conditions += [('date', '<', end_dt + DT_RANGE_FUDGE), ]  # end_dt is a hard limit

    try:
        next_time = select_min_from_joined(cursor, ['observability'],
                                           'date',
                                           conditions=conditions)
    # FIXME This catch needs to be updated to include the correct Exception
    except:
        return None

    return next_time


def get_fields_available(cursor, dt,
                         minimum_airmass=2.0,
                         resolution=15.):
    """
    Get the field_ids available for observation at a specified datetime.
    Parameters
    ----------
    cursor: :obj:`psycopg2.connection.cursor`
        psycopg2 cursor for interacting with the database.
    datetime: :obj:`datetime.datetime`
        datetime.datetime denoting the UTC time that we wish to investigate.
    minimum_airmass: :obj:`float`
        The airmass above which fields are unobservable. Defaults to 2.0.
    resolution: :obj:`float`, minutes
        The resolution to assume the almanac data points are stored in the DB
        with. Defaults to 15 (minutes).

    Returns
    -------
    field_ids: :obj:`list` of :obj:`int`
        A simple list of field_ids that are available at this time.
    """
    result = extract_from(cursor, 'observability',
                          columns='field_id',
                          conditions=[
                              ('airmass', '<=', minimum_airmass),
                              ('date', '<=', dt + datetime.timedelta(
                                  minutes=resolution/2.0) - DT_RANGE_FUDGE),
                              ('date', '>=', dt - datetime.timedelta(
                                  minutes=resolution/2.0) + DT_RANGE_FUDGE),
                          ])['field_id']
    return result


def get_airmass(cursor, field_ids, dt, resolution=15.):
    """
    Get the airmass corresponding to a particular field and datetime.

    Parameters
    ----------
    cursor: :obj:`psycopg2.connection.cursor`
    field_id : :obj:`int`
    dt : :obj:`datetime.datetime`

    Returns
    -------
    :obj:`float`
        The airmass at that field and datetime.
    """
    if not isinstance(field_ids, list):
        field_ids = [field_ids, ]
    if len(field_ids) == 0:
        return []

    result = extract_from(cursor, 'observability',
                          columns=['field_id', 'airmass'],
                          conditions=[
                              ('date', '>=',
                               dt - datetime.timedelta(
                                   minutes=resolution / 2.)),
                              ('date', '<=',
                               dt + datetime.timedelta(
                                   minutes=resolution / 2.)),
                              ('field_id', 'IN', field_ids) if
                              len(field_ids) > 1 else
                              ('field_id', '=', field_ids[0]),
                          ])
    return result


def find_fields_available(cursor, datetime_from, datetime_to=None,
                          field_list=None,
                          minimum_airmass=2.0, dark=True, grey=False,
                          resolution=15., active=True):
    """
    Get a list of all field_ids which will be available at some point during
    the specified period. Arguments are as for next_observable_period.

    Parameters
    ----------
    cursor: :obj:`psycopg2.connection.cursor`
        For interacting with the database
    datetime_from: :obj:`datetime.datetime`
        Datetime which to consider from. Must be within the bounds of the
        Almanac.
    datetime_to: :obj:`datetime.datetime`
        Datetime which to consider to. Defaults to None, in which case all
        available data points are used for the calculation.
    minimum_airmass: :obj:`float`
        Minimum airmass which will allow observing. Defaults to 2.0.
    dark, grey: :obj:`bool`
        Boolean values denoting whether to return the next observable period
        of grey time, dark time, or all night time (which corresponds to both
        dark and grey being False). Trying to pass dark=True and grey=True will
        raise a ValueError.
    active: :obj:`bool`
        Denotes which field is_active flag should be applied. Valid options are
        True, False and None (which returns all fields regardless of active
        status.

    Returns
    -------
    field_ids: :obj:`numpy.array`
        A numpy structured array with a single column 'field_id'.
    """

    # Input checking
    if datetime_to is None:
        datetime_to = select_max_from_joined(cursor, ['observability'],
                                             'date')
    if datetime_to < datetime_from:
        raise ValueError('datetime_from must occur before datetime_to')
    if dark and grey:
        raise ValueError('Only one of dark or grey may be True (or both may '
                         'be False to get all night time back)')
    if field_list is not None:
        if not isinstance(field_list, list):
            field_list = [field_list, ]

    # Read in the obs_start and obs_end times. It does this as follows:
    # - obs_start is the first time in the database after datetime_from and
    #   before datetime_to where the airmass is <= the limiting value
    # - obs_end is the first time in the database after obs_start and before
    #   dateimt_to where the airmass goes back above the limiting value

    conditions = [
        ('date', '>=', datetime_from - datetime.timedelta(minutes=resolution)),
        # ('date', '<=', datetime_to + datetime.timedelta(minutes=resolution)),
        ('airmass', '<=', minimum_airmass),
    ]
    if datetime_to:
        conditions += [('date', '<=', datetime_to +
                        datetime.timedelta(minutes=resolution))]
    if dark:
        conditions += [('dark', '=', True)]
    if grey:
        conditions += [('dark', '=', False)]
    if field_list is not None:
        field_list += [('field_id', 'IN', field_list)]

    if active is None:
        query_res = extract_from(cursor, 'observability', columns=['field_id',],
                                 conditions=conditions, distinct=True)
    else:
        query_res = extract_from_joined(cursor, ['observability', 'field'],
                                        columns=['field_id'],
                                        conditions=conditions+[
                                            ('is_active', '=', active),
                                        ], distinct=True)

    return query_res


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
    cursor: :obj:`psycopg2.connection.cursor`
    datetime_from: :obj:`datetime.datetime`
        Datetime which to consider from. Must be within the bounds of the
        Almanac.
    datetime_to: :obj:`datetime.datetime`
        Datetime which to consider to. Defaults to None, in which case all
        available data points are used for the calculation.
    minimum_airmass: :obj:`float`
        Minimum airmass which will allow observing. Defaults to 2.0.
    dark, grey: :obj:`bool`
        Boolean values denoting whether to return the next observable period
        of grey time, dark time, or all night time (which corresponds to both
        dark and grey being False). Trying to pass dark=True and grey=True will
        raise a ValueError.

    Returns
    -------
    obs_start, obs_end: :obj:`float`
        The start and end times when this field is observable. Note that
        datetimes are returned in pyephem format - this is for backwards-
        compatibility with the function in ts.scheduling.Almanac objects.
    """
    # Input checking
    if datetime_to is None:
        datetime_to = select_max_from_joined(cursor, ['observability'],
                                             'date', conditions=[
                ('field_id', '=', field_id),
            ])
    if datetime_to < datetime_from:
        raise ValueError('datetime_from must occur before datetime_to')
    if dark and grey:
        raise ValueError('Only one of dark or grey may be True (or both may '
                         'be False to get all night time back)')

    # Read in the obs_start and obs_end times. It does this as follows:
    # - obs_start is the first time in the database after datetime_from and
    #   before datetime_to where the airmass is <= the limiting value
    # - obs_end is the first time in the database after obs_start and before
    #   dateimt_to where the airmass goes back above the limiting value

    conditions = [
        ('field_id', '=', field_id),
        ('date', '>=', datetime_from - datetime.timedelta(minutes=resolution)
         - DT_RANGE_FUDGE),
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
                     resolution=15.,
                     airmass_delta=0.05):
    """
    Calculate how many hours this field is observable for between two
    datetimes.

    This function uses (almost) the same inputs and returns the same information
    as the same function intrinsic to ts.scheduling.Almanac objects.

    Parameters
    ----------
    cursor: :obj:`psycopg2.connection.cursor`
    datetime_from, datetime_to: :obj:`datetime.datetime`
        The datetimes between which we should calculate the number of
        observable hours remaining. These datetimes must be between the
        start and end dates of the almanac, or an error will be returned.
        datetime_to will default to None, such that the remainder of the
        almanac will be used.
    field_id: :obj:`int`
        The ID of the field to consider
    exclude_grey_time, exclude_dark_time: :obj:`bool`
        Boolean value denoting whether to exclude grey time or dark time
        from the calculation. Defaults to `exclude_grey_time=True`,
        `exclude_dark_time=False` (so only dark time will be counted as
        'observable'.) The legal combinations are:
        False, False (all night time is counted)
        False, True (grey time only)
        True, False (dark time only)
        Attempting to set both value to True will raise a ValueError, as
        this would result in no available observing time.
    minimum_airmass: :obj:`float`
        Something of a misnomer; this is actually the *maximum* airmass at which
        a field should be considered visible (a.k.a. the minimum altitude).
        Defaults to 2.0 (i.e. an altitude of 30 degrees). If hours_better
        is used, the comparison airmass will be the minimum of the airmass at
        datetime_from and minimum_airmass.
    hours_better: :obj:`bool`
        Optional Boolean, denoting whether to return only
        hours_observable which have airmasses superior to the airmass
        at datetime_from (True) or not (False). Defaults to False.
    resolution : :obj:`float`
        Resolution of the Almanac in minutes. Defaults to 15.
    airmass_delta : :obj:`float`
        Denotes the delta airmass that should be used to compute 
        hours_observable if hours_better=True. The hours_observable will be
        computed against a threshold airmass value of 
        (airmass_now + airmass_delta). This
        has the effect of 'softening' the hours_observable calculation for
        zenith fields, i.e. fields rapidly heading towards the minimum_airmass
        limit will be prioritized over those just passing through zenith.

    Returns
    -------
    hours_obs : :obj:`float`
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
        # [0] gives the only result, then [1] gives the airmass
        minimum_airmass = min(get_airmass(cursor, field_id,
                                          datetime_from)
                              [0][1] + airmass_delta,
                              minimum_airmass)
        logging.debug('Comparison airmass: %1.3f' % minimum_airmass)
    conditions += [('airmass', '<=', minimum_airmass)]

    hours_obs = resolution * count_from(cursor, 'observability',
                                        conditions=conditions) / 60.
    # You can also extract the data and count in Python - this isn't
    # any faster at the moment though
    # d = extract_from(cursor, 'observability', columns=['date',],
    #                  conditions=conditions)
    # hours_obs = resolution * len(d['date']) / 60.
    return hours_obs


def hours_observable_bulk(cursor, field_ids, datetime_from, datetime_to,
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
    cursor : :obj:`psycopg2.connection.cursor`
    datetime_from, datetime_to : :obj:`datetime.datetime`
        The datetimes between which we should calculate the number of
        observable hours remaining. These datetimes must be between the
        start and end dates of the almanac, or an error will be returned.
        datetime_to will default to None, such that the remainder of the
        almanac will be used.
    exclude_grey_time, exclude_dark_time: :obj:`bool`
        Boolean value denoting whether to exclude grey time or dark time
        from the calculation. Defaults to exclude_grey_time=True,
        exclude_dark_time=False (so only dark time will be counted as
        'observable'.) The legal combinations are:
        False, False (all night time is counted)
        False, True (grey time only)
        True, False (dark time only)
        Attempting to set both value to True will raise a ValueError, as
        this would result in no available observing time.
    hours_better: :obj:`bool`
        Optional Boolean, denoting whether to return only
        hours_observable which have airmasses superior to the airmass
        at datetime_now (True) or not (False). Defaults to False.

    Returns
    -------
    hours_obs: :obj:`float`
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

    if field_ids is None:
        raise ValueError('Please pass a list of field_ids')
    elif not isinstance(field_ids, list):
        field_ids = [field_ids, ]
    if len(field_ids) == 0:
        return []

    conditions = [
        ('field_id', 'IN', field_ids),
        ('date', '>=', datetime_from),
        ('sun_alt', '<=', ts.SOLAR_HORIZON)
    ]
    case_conditions = []
    if datetime_to:
        conditions += [('date', '<', datetime_to)]
    if exclude_grey_time:
        conditions += [('dark', '=', True)]
    if exclude_dark_time:
        conditions += [('dark', '=', False)]
    if hours_better:
        # Get the benchmark airmasses
        bench_airmass = get_airmass(cursor, field_ids, datetime_from,
                                    resolution=resolution)
        logging.debug('Found airmasses:')
        logging.debug(bench_airmass)
        # Build the case_condition for it
        case_conditions += [
            ('airmass', '<=',
             [('field_id', '=', x['field_id'],
               min(minimum_airmass, x['airmass'])) for
              x in bench_airmass if x['airmass'] < minimum_airmass],
             minimum_airmass)
        ]
        if len(case_conditions[0][2]) == 0:
            _ = case_conditions.pop(-1)
        logging.debug(case_conditions)
    else:
        conditions += [('airmass', '<=', minimum_airmass)]

    hours_obs = count_grouped_from_joined(cursor, ['observability'],
                                          'field_id',
                                          conditions=conditions,
                                          case_conditions=
                                          case_conditions).astype(
        dtype={"names": ["field_id", "count"],
               "formats": ['int64', 'float64']}
    )
    hours_obs['count'] = resolution * hours_obs['count'] / 60.

    return hours_obs
    # return hours_obs[hours_obs['field_id'] in [field_ids, ]]


def next_night_period(cursor, dt, limiting_dt=None,
                      dark=True, grey=False,
                      field_id=None):
    """
    Returns next period of 'night' (modulo observing conditions).

    Parameters
    ----------
    cursor: :obj:`psycopg2.connection.cursor`
        psycopg2 cursor for interacting with the database.
    dt: :obj:`datetime.datetime`
        Datetime to consider. Should be in UTC (but naive).
    dark, grey: :obj:`bool`
        Booleans denoting whether to consider only dark or grey time. Setting
        both to False will simply return 'night' time. Setting both to True
        will raise a ValueError.

    Returns
    -------
    night_start, night_end: :obj:`datetime.datetime`
        The start and end of the next 'night' period as naive datetime.datetime
        objects. These will be in UTC.
    """
    if dark and grey:
        raise ValueError('Cannot set dark and grey to both be true - only '
                         'pick one!')

    # Pick a field from the observability table - doesn't matter which one
    if field_id is None:
        fields = extract_from(cursor, 'field', columns=['field_id'],
                              distinct=True,
                              conditions=[('is_active', '=', True)])
        field_id = fields['field_id'][0]

    conditions = [('field_id', '=', field_id)]
    if dark:
        conditions += [('dark', '=', True)]
    elif grey:
        conditions += [('dark', '=', False)]
    if limiting_dt is not None:
        conditions += [('date', '<', limiting_dt)]

    # Try to get dark_start
    dark_start = select_agg_from_joined(cursor, ['observability'], 'min',
                                        'date',
                                        conditions=conditions + [
                                            ('date', '>=', dt),
                                            ('sun_alt', '<=', ts.SOLAR_HORIZON),
                                        ])
    if dark_start is None:
        return None, None

    conditions = [('field_id', '=', field_id)]
    conditions_combine = ['AND']
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

    dark_end = select_agg_from_joined(cursor, ['observability'], 'min', 'date',
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