# Set tiles to be observed

import logging
import datetime
from ....scripts.manipulate import update_rows


def execute(cursor, tile_pks, time_obs=None, hrs_better=None, airmass=None):
    """
    Set tiles as having been observed.

    Parameters
    ----------
    cursor:
        psycopg2 cursor for communicating with the database.
    tile_pks:
        The list of tile primary keys to set as observed. Note that these tiles
        will all have is_queued set to 'False' as a result (a tile cannot be
        queued if it has already been observed).
    time_obs:
        Optional; time of observation of the tile(s). Can either be a single
        datetime.datetime instance which will be applied to all tiles, or a list
        of datetime.datetime instances corresponding one-to-one with each tile
        in tile_pks. Defaults to None (so no time is recorded).

    Returns
    -------
    Nil. Updates are written to the database.
    """

    logging.info('Setting tiles as observed')

    # Make sure the target_ids is in list format
    tile_pks = list(tile_pks)
    if hrs_better is None:
        hrs_better = [None, ] * len(tile_pks)
    if airmass is None:
        airmass = [None, ] * len(tile_pks)
    if len(hrs_better) != len(tile_pks):
        raise ValueError('hrs_better must the same length as tile_pks')
    if len(airmass) != len(tile_pks):
        raise ValueError('airmass must the same length as tile_pks')

    # Make sure the datetimes are in list format
    if time_obs is not None:
        try:
            time_obs = list(time_obs)
            if len(time_obs) != len(tile_pks):
                raise ValueError('If passing a list of time_obs to '
                                 'makeTilesObserved, you *must* pass *one* '
                                 'datetime per tile')
        except TypeError:
            # Only a single datetime was passed
            time_obs = [time_obs, ] * len(tile_pks)

    # Form the data list
    data_list = list([[tile_pks[i], True, False,
                       hrs_better[i], airmass[i]] for i in
                      range(len(tile_pks))])
    # Update the rows
    update_rows(cursor, 'tile', data_list, columns=['tile_pk', 'is_observed',
                                                    'is_queued', 'hrs_better',
                                                    'airmass'])

    # Put in the observation times
    if time_obs is not None:
        data_obs = zip(tile_pks, time_obs)
        logging.debug(data_obs)
        update_rows(cursor, 'tiling_config', data_obs,
                    columns=['tile_pk', 'date_obs'])

    return
