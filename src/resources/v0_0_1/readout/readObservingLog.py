# Read in data from the observing_log table

from src.scripts.extract import extract_from_joined

import datetime
import logging

from taipan.scheduling import utc_local_dt

def execute(cursor, date_start=None, date_end=None,
            target_ids=None, field_list=None):
    """
    Read in observing log data.

    Note that the data is the target info as it stood when the target was
    *observed*. So, for example, the first observation of a target will show
    visits=0, repeats=0 and observations=0. This is the desired behaviour -
    it allows us to check the behaviour of the analysis routines.

    The post-observation target status can be determined using the success
    column of the returned array. If True, repeats will be incremented and
    done would be set (if not already). If False, visits will be incremented.
    Observations would be incremented in all cases.

    Parameters
    ----------
    cursor : :obj:`psycopg2.connection.cursor`
        For communication with the database
    date_start : :obj:`datetime.datetime`, optional
        Earliest date to consider
    date_end : :obj:`datetime.datetime`, optional
        Latest date to consider
    target_ids : :obj:`list` of :obj:`int`, optional
        Limit the returned log to matching targets only

        .. warning::
            Providing a large list of target_ids will make the database query
            very slow!
    field_list : :obj:`list` of :obj:`int`, optional
        Limit the returned log to matching fields only

    Returns
    -------
    obs_log : :obj:`numpy.array`
        The observing log. All columns from the following tables will be
        included in the output:

        - ``observing_log``
        - ``target``
        - ``tile``
        - ``tiling_config``
    """

    midday = datetime.time(12, 0, 0)

    if target_ids:
        target_ids = list(target_ids)
    if field_list:
        field_list = list(field_list)

    conditions = []
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
    if target_ids and len(target_ids) > 0:
        conditions += [('target_id', 'IN', target_ids)]
    if field_list and len(field_list) > 0:
        conditions += [('target_id', 'IN', target_ids)]

    logging.debug(conditions)

    obs_log = extract_from_joined(cursor, ['observing_log', 'target', 'tile',
                                           'tiling_config'])

    return obs_log
