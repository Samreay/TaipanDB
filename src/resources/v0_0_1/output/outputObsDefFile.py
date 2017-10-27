# Output tile(s) to observing definition file(s)

import datetime
import pytz
import json
import numpy as np
import logging

from src.resources.v0_0_1.readout import readTiles as rT
from src.resources.v0_0_1.readout import readTilePK as rTpk
from src.resources.v0_0_1.readout import readTileObservingInfo as rTOI

from taipan.core import JSON_DTFORMAT_NAIVE, JSON_DTFORMAT_TZ
import taipan.scheduling as ts

def execute(cursor, tile_pks=None, unobserved=None, unqueued=None,
            config_time=datetime.datetime.now(),
            obs_time=None,
            output_dir='.',
            local_tz=ts.UKST_TIMEZONE):
    """
    For all tiles matching the input parameters, export an observing definition
    file.
    
    Parameters
    ----------
    cursor : :obj:`psycopg2.connection.cursor`
        psycopg2 cursor for interacting with the database
    tile_pks : :obj:`list` of :obj:`int`, optional
        List of tile_pks we are interested in. Defaults to None, at which
        point all tiles matching any other criteria will have observing
        files returned.
    unobserved : :obj:`bool`, optional
        Whether to return only unobserved tiles (True), observed tiles (False),
        or ignore the observed status. Defaults to None.
    unqueued : :obj:`bool`, optional
        As for unobserved, but considers the queued status.
    config_time: :obj:`datetime.datetime`, timezone-aware, optional
        The time of generation for this file. Defaults to 
        datetime.datetime.now(), and a timezone from the core Taipan config.
    obs_time: :obj:`datetime.datetime`, timezone-aware, optional
        As for config_time, but denotes when the tile is planned to be/is
        observed. If a tile has been observed, the noted observation time
        from the database will be used instead. This value will be broadcast
        across all tiles returned which do not have an observing time 
        defined in the database.
    output_dir: :obj:`str`, optional
        The file path where the output JSON files should be written. Defaults
        to '.' (i.e. the current working directory).
    local_tz: :obj:`pytz.timezone`, optional
        Timezone used to make generate `config_time` and `obs_time` if not
        passed. Defaults to :any:`taipan.scheduling.UKST_TIMEZONE`.

    Returns
    -------
    file_names: :obj:`list` of :obj:`str`
        A list of absolute paths to the output files.
    """

    file_names = []

    # Pull the relevant tiles out of the database
    conditions = []
    if tile_pks is not None:
        conditions += [('tile_pk', 'IN', tile_pks), ]
    if unobserved is not None:
        conditions += [('is_observed', '=', not unobserved), ]
    if unqueued is not None:
        conditions += [('is_queued', '=', not unqueued), ]
    tile_pks = rTpk.execute(cursor, conditions=conditions)
    tiles = rT.execute(cursor, tile_pks=tile_pks)[0]

    # Get the tile_obs_log for observed tiles
    if unobserved is not None:
        tile_obs_log = rTOI.execute(cursor)

    for tile in tiles:
        json_dict = tile.generate_json_dict()
        json_dict['origin'][0]['execDate'] = local_tz.localize(
            config_time
        ).strftime(
            JSON_DTFORMAT_TZ
        )
        # json_dict['origin'][0]['execDate'] = config_time.astimezone(
        #     local_tz
        # )
        obs_time_log = [_['date_obs'] for _ in tile_obs_log if
                        _['tile_pk'] == tile.pk][0]
        if obs_time_log is None:
            obs_time_log = obs_time
        if tile.pk in tile_obs_log['tile_pk']:
            json_dict['fieldCentre']['UT'] = tile_obs_log[
                tile_obs_log['tile_pk'] == tile.pk
            ]['date_obs'][0].strftime(JSON_DTFORMAT_NAIVE)
        elif obs_time is not None:
            json_dict['fieldCentre']['UT'] = obs_time.strftime(
                JSON_DTFORMAT_NAIVE
            )
        with open(output_dir + '/' +
                  '%s_tile%7d_field%5d_config.json' %
                                  (local_tz.localize(
                                       obs_time_log
                                   ).strftime(
                                       JSON_DTFORMAT_NAIVE
                                   ), tile.pk, tile.field_id,
                                   ),
                  'w') as fileobj:
            logging.debug('Writing %s' % fileobj.name)
            json.dump(json_dict, fileobj, indent=2)
            file_names.append(fileobj.name)

    return file_names
