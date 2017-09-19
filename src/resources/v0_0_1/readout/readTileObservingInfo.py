import logging
import sys
import os
from ....scripts.extract import extract_from_joined
from taipan.core import TaipanTile

from src.resources.v0_0_1.readout import readAlmanacStats as rAS

from numpy.lib.recfunctions import append_fields


def execute(cursor, field_ids=None):
    """
    Read information about when tiles have been observed from the database

    Parameters
    ----------
    cursor: :obj:`psycopg2.connection.cursor`
        psycopg2 cursor for interacting with the database.

    Returns
    -------
    obs_tile_info: :obj:`numpy.array`
        A structured numpy array with all the information you could want about
        which tile was observed when.
    """

    conditions = [
        ('(', 'is_observed', '=', True, ''),
        ('', 'date_obs', 'IS NOT', 'NULL', ')'),
    ]
    combine = ['OR']

    if field_ids is not None:
        field_ids = list(field_ids)
        conditions += [
            ('field_id', 'IN', field_ids)
        ]
        combine += ['AND']

    obs_tile_info = extract_from_joined(cursor, ['field', 'tile',
                                                 'tiling_config',
                                                 'tiling_info'],
                                        columns=['ra', 'dec',
                                                 'field_id', 'tile_pk',
                                                 'tile_id',
                                                 'date_config', 'date_obs',
                                                 'prior_sum', 'n_sci_rem',
                                                 'hrs_better', 'airmass',
                                                 ],
                                        conditions=conditions,
                                        conditions_combine=combine)

    # We'd like the airmass information as well
    # We need to do a case-conditions extract for that
    case_conditions = [

    ]
    
    return obs_tile_info
