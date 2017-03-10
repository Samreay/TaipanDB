# Routine for adding information to the observing log

import datetime

from src.resources.v0_0_1.readout import readScienceTile as rScTi
from src.resources.v0_0_1.readout import readScienceTypes as rScTy

from src.scripts.manipulate import insert_many_rows

import numpy as np
from numpy.lib.recfunctions import append_fields

def execute(cursor, tile_pk, target_list, success_targets,
            datetime_at=datetime.datetime.now()):
    """
    Add data in to the observing_log table.

    Parameters
    ----------
    cursor : psycopg2 connection.cursor object
        Required for communicating with the database
    tile_pk : int
        The primary key of the tile we're adding to the observing log.
    target_list : list of ints
        The list of science targets that were observed on this tile. This list
        must match the list of targets assigned to the tile in the database,
        otherwise a ValueError will be thrown. This list needs to be provided
        separately, as opposed to just generated from the database, so it can
        be matched against success_targets.
    success_targets : list of Booleans
        List of Booleans, denoting whether this observation led to 'success'
        for each target in target_list. There is a one-to-one correspondence
        between the lists.
    datetime_at : datetime.datetime object
        Datetime at which the tile was observed. The datetime should be
        expressed in UTC; the datetime object itself should be timezone-naive.

    Returns
    -------
    Nil. Rows are written into the observing_log database table.
    """
    # Input checking
    tile_pk = int(tile_pk)
    target_list = list(target_list)
    success_targets = list(success_targets)
    if len(target_list) != len(success_targets):
        raise ValueError('target_list and success_targets must have the same '
                         'length')

    # Read in the targets on this tile - make sure target_list matches
    # this
    targets_by_db = rScTi.execute(cursor)
    if set(targets_by_db) != set(target_list):
        raise ValueError("The target_list you provided doesn't match the "
                         "target list generated by the DB for tile %d" %
                         tile_pk)

    # Read in the existing target information for the targets observed
    tgt_info = rScTy.execute(cursor, target_ids=target_list)
    # Sort the input success_targets lists s.t. it matches the ordering of
    # tgt_info
    tgt_info.sort(order='target_id')
    tgt_ordering = np.argsort(target_list)
    success_targets = list(np.asarray(success_targets)[tgt_ordering])

    # Append the extra necessary columns to the tgt_info array
    # tile_pk
    tgt_info = append_fields(tgt_info, 'tile_pk', [tile_pk]*len(tgt_info),
                             dtypes=int, usemask=False)
    # success
    tgt_info = append_fields(tgt_info, 'success', success_targets,
                             dtypes=bool, usemask=False)

    # Write the information back into the observing_log database
    insert_many_rows(cursor, 'observing_log',
                     tgt_info[['target_id',
                               'is_h0_target',
                               'is_vpec_target',
                               'is_prisci_vpec_target',
                               'is_full_vpec_target',
                               'is_lowz_target',
                               'col_gi',
                               'col_JK',
                               'is_nir',
                               'is_lrg',
                               'is_iband',
                               'zspec',
                               'visits',
                               'repeats',
                               'priority',
                               'difficulty'
                               'done',
                               'success',
                               'tile_pk']],  # Names & order must match table
                                             # definition
                     )

    return
