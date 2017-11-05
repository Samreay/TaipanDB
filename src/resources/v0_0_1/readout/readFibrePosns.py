# Read-out every fibre position stored in the database
# Useful for fibre stretch studies

import logging
from ....scripts.extract import extract_from_joined
from taipan.core import TaipanTile, targets_in_range, TILE_DIAMETER

import numpy as np
from matplotlib.cbook import flatten


def execute(cursor):
    """
    Extract and return data on fibre positions in the database.

    This function has no real role in simulation and/or live operations;
    however, the data it outputs is useful for fibre stretch studies, if the
    tiles in the database have been generated using some sort of repicking
    algorithm to attempt to optimise fibre travel distance/time.

    Parameters
    ----------
    cursor : :obj:`psycopg2.connection.cursor`
        psycopg2 cursor for interacting with the database

    Returns
    -------
    fibre_posns : :obj:`numpy.array`.
        Data on fibre posns, each row giving the following:

        - `target_id`
        - `field_id`
        - `bug_id`
        - `target_id`
        - `ra`
        - `dec`
    """

    fibre_posns = extract_from_joined(cursor,
                                      ['target_field', 'science_target',
                                       'target', 'tile'],
                                      conditions=[('target_id', '!=', -1), ],
                                      columns=['tile_pk', 'field_id', 'bug_id',
                                               'target_id', 'ra', 'dec'])

    return fibre_posns
