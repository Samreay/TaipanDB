# Set tiles to be observed

import logging
import datetime
from ....scripts.manipulate import update_rows


def execute(cursor, tile_pks, time_obs=datetime.datetime.now()):
    """
    Set tiles as having been observed.

    Parameters
    ----------
    cursor:
        psycopg2 cursor for communicating with the database.
    tile_pks:
        The list of tile primary keys to set as observed.
    time_obs:
        Optional; time of observation of the tile(s). Can either be a single
        datetime.datetime instance which will be applied to all tiles, or a list
        of datetime.datetime instances corresponding one-to-one with each tile
        in tile_pks. Defaults to datetime.datetime.now().

    Returns
    -------
    Nil. Updates are written to the database.
    """

    logging.info('Setting tiles as observed')

    # Make sure the target_ids is in list format
    target_ids = list(tile_pks)

    # Make sure the datetimes are in list format
    try:
        time_obs = list(time_obs)
        if len(time_obs) != len(tile_pks):
            raise ValueError('If passing a list of time_obs to '
                             'makeTilesObserved, you *must* pass *one* datetime'
                             ' per tile')
    except TypeError:
        # Only a single datetime was passed
        time_obs = [time_obs, ] * len(tile_pks)


    # Form the data list
    data_list = list([[tile_pk, True] for tile_pk in tile_pks])
    # Update the rows
    update_rows(cursor, 'tile', data_list, columns=['tile_pk', 'is_observed'])

    # Put in the observation times
    data_obs = zip(tile_pks, time_obs)
    logging.debug(data_obs)
    update_rows(cursor, 'tiling_config', data_obs,
                columns=['tile_pk', 'date_obs'])

    return
