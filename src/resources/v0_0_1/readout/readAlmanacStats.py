# Collection of functions for reading Almanacs which have been
# inserted into the database
# This should be faster than using in-memory Almanacs

from src.scripts.extract import extract_from, select_min_from_joined
import taipan.scheduling as ts
import numpy as np


def next_observable_period(cursor, field_id, datetime_from, datetime_to=None,
                           minimum_airmass=2.0):
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

    # Read in the obs_start and obs_end times. It does this as follows:
    # - obs_start is the first time in the database after datetime_from and
    #   before datetime_to where the airmass is <= the limiting value
    # - obs_end is the first time in the database after obs_start and before
    #   dateimt_to where the airmass goes back above the limiting value

    conditions = [
        ('field_id', '=', field_id),
        ('date', '>=', datetime_from),
    ]
    if datetime_to:
        conditions += [('date', '<=', datetime_to)]

    # Try to get obs_start
    obs_start = select_min_from_joined(cursor, ['observability'], 'date',
                                       conditions=conditions)

    if obs_start is None:
        # No time left, return double None
        return None, None

    obs_end

    # Extract the observability data
    data = extract_from(cursor, 'observability',
                        columns=['date', 'airmass'],
                        conditions=conditions).sort(
        axis=0, order='date',
    )

    # Compute obs_start
    try:
        obs_start = np.min(data[data['airmass'] <= minimum_airmass]['date'])
    except ValueError:
        # No obs_start, return None and None
        return None, None

    # Compute obs_end
    try:
        obs_end = np.min(data[np.logical_and(data['airmass'] > minimum_airmass,
                                             data['date'] > obs_start)]['date'])
    except ValueError:
        obs_end = np.max(data['date'])

    return obs_start, obs_end

    pass


def hours_observable(field, datetime_from, datetime_to,
                     exclude_grey_time=True,
                     exclude_dark_time=False,
                     timezone=ts.UKST_TIMEZONE,
                     hours_better=True):
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
    dark_almanac:
        An instance of DarkAlmanac used to compute whether time is grey or
        dark. Defaults to None, at which point a DarkAlmanac will be
        constructed (if required).
    tz:
        The timezone that the (naive) datetime objects are being passed as.
        Defaults to taipan.scheduling.UKST_TIMEZONE.
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
    pass
