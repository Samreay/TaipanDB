# Output an observing index file for the given datetime range or tiles

import numpy as np
import os
import datetime
import logging

import taipandb.scripts.extract as dbex

import taipandb.resources.stable.readout.readAlmanacStats as rAS
import taipandb.resources.stable.readout.readTilePK as rTPK
import taipandb.resources.stable.readout.readTileObservingInfo as rTOI
from taipandb.resources.stable.readout import OBS_DEF_FILENAME_REGEX, \
    OBS_DEF_FILENAME_DTFMT, OBS_IND_FILE_LINE

import taipan.scheduling as ts

def execute(cursor,
            datetime_from=None, datetime_to=None,
            tile_list=None,
            output_dir='.',
            dark=True, grey=False, minimum_airmass=2.0,
            resolution=15., localtz=ts.UKST_TIMEZONE):
    """
    Output a tile observing index for the given time or tiles.

    This function can compute an observing index file for a given time
    range, OR a given list of tile_pks. An attempt to apply both criteria
    (or neither) will result in a ValueError being raised.

    If a time range is given, only queued tiles will be planned for.

    This function will usually be called in conjunction with
    :any:`outputObsDefFile.execute` to generate the observing plan for an
    evening.

    Parameters
    ----------
    cursor : :obj:`psycopg.connection.cursor`
        Cursor for database communication
    datetime_from : :obj:`datetime.datetime`, optional
        Start datetime for observing index. Defaults to None. If passed,
        should be a naive object representing UTC.
    datetime_to : :obj:`datetime.datetime`, optional
        End datetime for observing index. Defaults to None. If passed,
        should be a naive object representing UTC.
    tile_list : :obj:`list` of :obj:`int`
        List of tile primary keys to generate an observing index for. Defaults
        to None.
    output_dir : :obj:`str`
        File path to save the observing index file to. Can be relative or
        absolute. Defaults to `"."` (i.e. the present working directory). This
        directory should be the same as the one containing the configuration
        files, as they will be searched for to complete the observing index
        file.
    dark, grey: :obj:`bool`
        As part of this function, the system needs to calculate the earliest
        and latest time a tile could be observed. The keywords `dark` and
        `grey` denote whether dark and/or grey time is considered appropriate
        for observing. The default is `dark=True, grey=False` (i.e. the
        Taipan standard).

    Returns
    -------
    output_filename: :obj:`str`
        The file name of the generated observing index file.
    """

    # Input checking
    if (datetime_from is None) != (datetime_to is None):
        raise ValueError('Must provide both datetime_from and datetime_to, '
                         'or neither')
    if datetime_to and datetime_from and tile_list:
        raise ValueError('Should only provide a start and end time OR '
                         'a list of tile PKs')
    if datetime_to is None and datetime_from is None and tile_list is None:
        raise ValueError('Must provide either a start and end time, '
                         'or a list of tile PKs')

    # Construct the DB query conditions
    if datetime_to:
        conditions = [('date_obs', '>=', datetime_from - rAS.DT_RANGE_FUDGE),
                      ('date_obs', '<=', datetime_to + rAS.DT_RANGE_FUDGE)]
        tile_list = rTPK.execute(cursor, conditions=conditions,
                                 tables_to_join=['tiling_config', ])
    tile_list = list(tile_list)

    tile_obs_log = rTOI.execute(cursor)
    tile_obs_log = tile_obs_log[np.in1d(tile_list, tile_obs_log['tile_pk'])]
    tile_obs_log.sort(order='date_obs')

    # For each tile in the obs_log, we need to construct a line of an
    # observing index file to write
    # Line consists of:
    # - Ideal(i.e.scheduled) time for observation;
    # - Earliest possible time for observation;
    # - Latest possible time for observation;
    # - Tile id (pk)
    # - Absolute file path for configuration file
    write_str = ''
    files_in_dir = [OBS_DEF_FILENAME_REGEX.match(_) for _ in
                    os.listdir(output_dir)]
    logging.info('Obs def file matches:')
    logging.info([_.group(0) for _ in files_in_dir if _ is not None])

    time_conditions_generic = [
        ('(', 'airmass', '>=', minimum_airmass, ''),
        ('', 'sun_alt', '<', ts.SOLAR_HORIZON, ''),
    ]
    if dark:
        time_conditions_generic += [('', 'dark', '=', False, ')')]
    if grey:
        time_conditions_generic += [('', 'dark', '=', True, ')')]
    if not dark and not grey:
        time_conditions_generic[-1] = ('', 'sun_alt', '<',
                                       ts.SOLAR_HORIZON, ')'),
    time_conditions_generic_comb = ['OR'] * (len(time_conditions_generic) - 1)
    for tile in tile_obs_log:
        # Earliest time
        earliest_time = dbex.select_max_from_joined(
            cursor,
            ['observability', ],
            'date',
            conditions=
            time_conditions_generic +
            [('date', '<', tile['date_obs']),
             ('field_id', '=', tile['field_id']), ],
            conditions_combine=time_conditions_generic_comb + ['AND', 'AND']
        )
        if earliest_time is None:
            earliest_time = tile['date_obs']
        earliest_time += datetime.timedelta(minutes=resolution)
        # Look for & avoid floating point errors
        if earliest_time > tile['date_obs']:
            earliest_time = tile['date_obs']

        # Latest time
        latest_time = dbex.select_min_from_joined(
            cursor,
            ['observability', ],
            'date',
            conditions=
            time_conditions_generic +
            [('date', '>', tile['date_obs']),
             ('field_id', '=', tile['field_id']), ],
            conditions_combine=time_conditions_generic_comb + ['AND', 'AND']
        )
        if latest_time is None:
            latest_time = tile['date_obs']
        latest_time -= datetime.timedelta(minutes=resolution)
        # Look for & avoid floating point errors
        if latest_time < tile['date_obs']:
            latest_time = tile['date_obs']

        # Find config file
        matching_confs = [_ for _ in files_in_dir if
                          _ and int(_.group('tilepk')) == tile['tile_pk']]
        try:
            conf_file = matching_confs[0].group(0)
        except IndexError:
            conf_file = '--'

        # Add string for this file
        # Note that all times need to be converted to local
        str_to_add = OBS_IND_FILE_LINE % (
            ts.localize_utc_dt(tile['date_obs'],
                               tz=localtz).strftime('%H%M'),
            ts.localize_utc_dt(earliest_time,
                               tz=localtz).strftime('%H%M'),
            ts.localize_utc_dt(latest_time,
                               tz=localtz).strftime('%H%M'),
            int(tile['tile_pk']),
            conf_file
        )
        # logging.info('Adding string:')
        # logging.info(str_to_add)
        write_str += str_to_add

    logging.info('Output string:')
    logging.info(write_str)

    with open(output_dir + '/observing_index.ind', 'w') as outfile:
        outfile.write(write_str)
        output_filename = os.path.realpath(outfile.name)

    return output_filename
