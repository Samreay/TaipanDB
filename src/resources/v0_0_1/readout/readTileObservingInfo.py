import logging
import sys
import os
from ....scripts.extract import extract_from_joined
from taipan.core import TaipanTile


def execute(cursor, field_ids=None):
    """
    Read information about when tiles have been observed from the database

    Parameters
    ----------
    cursor:
        psycopg2 cursor for interacting with the database.

    Returns
    -------
    obs_tile_info:
        A structured numpy array with all the information you could want about
        which tile was observed when.
    """

    obs_tile_info = extract_from_joined(cursor, ['field', 'tile', 'tiling_config'],
                                        columns=['ra', 'dec',
                                                 'field_id', 'tile_pk',
                                                 'date_config', 'date_obs'],
                                        conditions=[
                                            ('is_observed','=',True),
                                            ('date_obs', 'IS NOT', 'NULL'),
                                        ],
                                        conditions_combine='OR')
    
    return obs_tile_info