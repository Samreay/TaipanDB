# Read-out every fibre position stored in the database
# Useful for fibre stretch studies

import logging
from ....scripts.extract import extract_from_joined
from readCentroids import execute as rCexec
from taipan.core import TaipanTile, targets_in_range, TILE_DIAMETER

import numpy as np
from matplotlib.cbook import flatten


def execute(cursor):
    """
    Extract and return data on fibre positions in the database.

    Parameters
    ----------
    cursor:
        psycopg2 cursor for interacting with the database

    Returns
    -------
    fibre_posns:
        Data on fibre posns, each row giving the following:
    """

    fibre_posns = extract_from_joined(cursor,
                                      ['target_field', 'science_target',
                                       'target', 'tile'],
                                      conditions=[('target_id', '!=', -1), ],
                                      columns=['tile_pk', 'field_id', 'bug_id',
                                               'target_id', 'ra', 'dec'])

    return fibre_posns
