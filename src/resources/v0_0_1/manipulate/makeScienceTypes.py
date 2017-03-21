# Increment the visit count for passed targets
# (i.e. target was observed, but not well enought to be counted as a completion

import logging
from ....scripts.manipulate import increment_rows, update_rows


def execute(cursor, target_ids, is_h0_target, is_vpec_target,
            is_lowz_target):
    """
    Update target types in the database.

    It is important to note that *every* type flag will be updated for
    *every* target passed in. Therefore, the inputs need to be explicit
    about whether a target is of that type or not (i.e. False in
    is_h0_target corresponds to a target *definitely* not being H0, rather
    than unknown).

    Parameters
    ----------
    cursor:
        psycopg2 cursor for communicating with the database.
    target_ids:
        The list of target_ids to do priority updates for.
    is_h0_target, is_vpec_target, is_lowz_target:
        Lists of Booleans, denoting whether the corresponding target is of
        that type. Lists must have the same length as target_ids, and must
        have a one-to-one correspondence with that list.

    Returns
    -------
    Nil. Types are written into the database

    """

    logging.info('Writing science types to database')

    # Make sure the target_ids is in list format
    target_ids = list(target_ids)
    is_h0_target = list(is_h0_target)
    is_vpec_target = list(is_vpec_target)
    is_lowz_target = list(is_lowz_target)

    if len(target_ids) == 0:
        return
    if len(target_ids) != len(is_h0_target):
        raise ValueError('target_ids and is_h0_target must be of the same '
                         'length!')
    if len(target_ids) != len(is_vpec_target):
        raise ValueError('target_ids and is_vpec_target must be of the same '
                         'length!')
    if len(target_ids) != len(is_lowz_target):
        raise ValueError('target_ids and is_lowz_target must be of the same '
                         'length!')

    # Relate database columns to input lists
    type_dict = {
        'is_h0_target': is_h0_target,
        'is_vpec_target': is_vpec_target,
        # 'is_lowz_target': is_lowz_target,
    }

    for t in type_dict.keys():
        update_array = [[target_ids[i], type_dict[t][i]] for
                        i in range(len(target_ids))]
        update_rows(cursor, 'science_target', update_array,
                    columns=['target_id', t])

    return
