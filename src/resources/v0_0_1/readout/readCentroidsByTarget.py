# Get a list of fields (centroids) that contain targets of a particular type

import logging
from ....scripts.extract import extract_from, extract_from_left_joined, \
    count_grouped_from_joined
from readCentroids import execute as rCexec
from taipan.core import TaipanTile, targets_in_range, TILE_DIAMETER

import numpy as np
from matplotlib.cbook import flatten


def execute(cursor, tgt_type, unobserved=True,
            threshold_value=1, assigned_only=True):
    """
    Calculate which field(s) contain targets of a particular type, or
    group of types.

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
    assigned_only : Boolean, optional
        Only consider targets which are assigned to a pending tile (True).
        Defaults to True.

    Returns
    -------
    field_ids:
        A list of fields that contain targets of the specified type(s).
    """
    threshold_value = int(threshold_value)
    if threshold_value < 1:
        raise ValueError('threshold_value must be >= 1')

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
        # conditions += [
        #     ('done', 'IS', 'NULL')
        # ]
        # 170518: Change to hurry=up criteria to avoid fillers being used
        conditions += [
            ('success', '=', False)
        ]

    for t in tgt_type:
        conditions += [(t, '=', True)]

    if assigned_only:
        conditions += [
            ('is_observed', '=', False)
        ]

        if threshold_value == 1:
            fields_affected = extract_from_left_joined(cursor,
                                                       ['tile',
                                                        'target_field'
                                                        'science_target', ],
                                                       ['tile_pk', 'target_id'],
                                                       columns=['field_id'],
                                                       conditions=conditions,
                                                       distinct=True)
        else:
            count = count_grouped_from_joined(cursor,
                                              ['tile', 'target_field',
                                               'science_target'],
                                              'field_id',
                                              conditions=conditions)
            fields_affected = count[count['count'] >= threshold_value]
    else:
        # Read out the relevant list of fields
        if threshold_value == 1:
            fields_affected = extract_from_left_joined(cursor,
                                                       ['target_posn',
                                                        'science_target', ],
                                                       'target_id',
                                                       columns=['field_id'],
                                                       conditions=conditions,
                                                       distinct=True)
        else:
            count = count_grouped_from_joined(cursor,
                                              ['target_posn', 'science_target'],
                                              'field_id',
                                              conditions=conditions)
            fields_affected = count[count['count'] >= threshold_value]

    return list(fields_affected['field_id'])

