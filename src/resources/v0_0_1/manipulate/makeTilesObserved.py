# Set tiles to be observed

import logging
from ....scripts.manipulate import update_rows


def execute(cursor, tile_pks):
    """
    Set tiles as having been observed.

    Parameters
    ----------
    cursor:
        psycopg2 cursor for communicating with the database.
    tile_pks:
        The list of tile primary keys to set as observed.

    Returns
    -------
    Nil. Updates are written to the database.
    """

    logging.info('Setting tiles as observed')

    # Make sure the target_ids is in list format
    target_ids = list(tile_pks)

    # Form the data list
    data_list = list([[tile_pk, True] for tile_pk in tile_pks])

    # Update the rows
    update_rows(cursor, 'tile', data_list, columns=['tile_pk', 'is_observed'])

    return
