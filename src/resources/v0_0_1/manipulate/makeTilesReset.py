# Set tiles to be observed

import logging
import datetime
from ....scripts.manipulate import update_rows, update_rows_all


def execute(cursor, tile_pks=None):
    """
    'Reset' a tile - that is, set it to be unqueued

    Parameters
    ----------
    cursor:
        psycopg2 cursor for communicating with the database.
    tile_pks:
        Optional; list of tile primary keys to set as unqueued. Defaults to
        None, at which point all tiles will be unqueued (a useful end-of-night
        cleanup task).

    Returns
    -------
    Nil. Updates are written to the database.
    """

    logging.info('Setting tiles as un-queued')

    # Make sure the target_ids is in list format
    if tile_pks is not None:
        tile_pks = list(tile_pks)

    if tile_pks is None:
        update_rows_all(cursor, 'tile', [False, ], columns=['is_queued'])
    else:
        # Form the data list
        data_list = list([[tile_pk, False] for tile_pk in tile_pks])
        # Update the rows
        update_rows(cursor, 'tile', data_list, columns=['tile_pk', 'is_queued'])

    return
