import logging
import psycopg2
import numpy as np

from taipandb.scripts.delete import delete_rows


def execute(cursor, tile_list=None, field_list=None,
            obs_status=None, queue_status=None):
    """
    Drop a specified set of tiles from the tile database table.

    Parameters
    ----------
    cursor: :obj:`psyopg2.connection.cursor`
        psycopg2 cursor for interacting with the database
    tile_list: :obj:`list` of :obj:`taipan.core.TaipanTile`
        Optional; list of tile PKs to delete. Defaults to None, such that
        tile_pk does not appear as a filtering condition.
    field_list: :obj:`list` of :obj:`int`
        Optional; list of field IDs to delete all tiles for. Defaults to None,
        at which point tiles will be removed regardless of field.
    obs_status: :obj:`bool`
        Optional; observation status of tiles to be removed. False correlates
        to is_observed=False, True correlates to is_observed=True. Defaults to
        None, at which point tiles will be removed irrespective of observing
        status.
    queue_status: :obj:`bool`
        Optional; queue status of tiles to be removed. False correlates
        to is_queued=False, True correlates to is_queued=True. Defaults to
        None, at which point tiles will be removed irrespective of queue
        status.

    Returns
    -------
    :obj:`None`
        Rows are dropped from the 'tile' database table.
    """
    # Input checking
    if obs_status is not None:
        obs_status = bool(obs_status)

    # Form the conditions string
    conditions = []
    if tile_list:
        conditions.append(('tile_pk', 'IN', tile_list))
    if field_list:
        conditions.append(('field_id', 'IN', field_list))
    if obs_status is not None:
        conditions.append(('is_observed', '=', obs_status))
    if queue_status is not None:
        conditions.append(('is_queued', '=', obs_status))

    # Do the deletion
    delete_rows(cursor, 'tile', conditions=conditions)
    return
