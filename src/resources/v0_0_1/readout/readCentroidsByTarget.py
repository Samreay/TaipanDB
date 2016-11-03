# Get a list of fields (centroids) that contain targets of a particular type

import logging
from ....scripts.extract import extract_from, extract_from_left_joined
from readCentroids import execute as rCexec
from taipan.core import TaipanTile, targets_in_range, TILE_DIAMETER

import numpy as np
from matplotlib.cbook import flatten


def execute(cursor, tgt_type, unobserved=True):
    """
    Calculate which fields will be affected by changes to the fields/tiles
    passed as inputs, and return a list of them.

    The user should pass ONLY field_list OR tile_list. Passing neither will
    raise a ValueError; passing both will cause field_list to take priority.

    Parameters
    ----------
    cursor:
        psycopg2 cursor for interacting with the database
    tgt_type : {'is_H0_target', 'is_lowz_target', 'is_vpec_target'}, or a
               list of any combination of these
        The target type(s) to search against.
    unobserved : Boolean, optional
        Only consider unobserved targets (True) or all targets (False) when
        finding the relevant fields. Defaults to True.

    Returns
    -------
    field_ids:
        A list of fields that contain targets of the specified type(s).
    """

    target_types = [
        'is_lowz_target',
        'is_h0_target',
        'is_vpec_target',
    ]

    if not isinstance(tgt_type, list):
        tgt_type = [tgt_type, ]

    if not np.all(np.asarray(map(lambda x: x in target_types, tgt_type))):
        raise ValueError('tgt_type must be one of, or a list of, %s' %
                         str(target_types))

    conditions = []

    if unobserved:
        conditions += [('done', '=', False)]

    # Read out the relevant list of fields
    fields_affected = extract_from_left_joined(cursor, ['target_posn',
                                                        'science_target'],
                                               'target_id',
                                               columns=['field_id'],
                                               conditions=conditions,
                                               distinct=True)

    return list(fields_affected['field_id'])

