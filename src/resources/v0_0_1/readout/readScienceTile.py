import logging
import sys
import os
from ....scripts.extract import extract_from_joined
from taipan.core import TaipanTarget


def execute(cursor, tile_pk):
    """
    Retrieve an array of target IDs and associated types.

    Parameters
    ----------
    cursor:
        psycopg2 cursor for communicating with the database
    tile_pk:
        The primary key of the tile to be investigated. If this tile does not
        exist, the function will return an empty list of target_ids.

    Returns
    -------
    targets:
        A list of target_ids which sit on this tile. Note that only *science*
        targets will be returned (this is guaranteed by joining the tile_pk
        table against the science_target table).

    """
    logging.info('Reading science targets (types) from database')

    tile_pk = int(tile_pk)

    conditions = [('tile_pk', '=', tile_pk), ]

    targets_db = extract_from_joined(cursor, ['target_field', 'science_target'],
                                     conditions=conditions,
                                     columns=['target_id'])

    logging.info('Extracted %d targets from database' % len(targets_db))
    return targets_db
