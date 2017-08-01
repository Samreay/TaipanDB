import logging
import sys
import os
from ....scripts.extract import extract_from_joined
from taipan.core import TaipanTarget


def execute(cursor, target_ids=None, active_only=True):
    """
    Retrieve an array of target IDs and associated types.

    Parameters
    ----------
    cursor:
        psycopg2 cursor for communicating with the database
    target_ids:
        Optional list of target_ids to return. Defaults to None, at which point
        all targets in the database are returned.

    Returns
    -------
    targets_db:
        A numpy structured array of data from the science_target table in the
        database. Each row is of the format [target_id, is_vpec_target,
        is_HO_target, is_lowz_target].

    """
    logging.info('Reading science targets (types) from database')

    if target_ids is not None:
        if len(target_ids) == 0:
            return []
        conditions = [('target_id', 'IN', tuple(target_ids)), ]
    elif active_only:
        conditions = [('is_active', '=', True)]
    else:
        conditions = None

    targets_db = extract_from_joined(cursor, ['science_target', 'target'],
                                     conditions=conditions,
                                     columns=['target_id', 'ra', 'dec',
                                              'is_vpec_target',
                                              'is_h0_target', 'is_lowz_target',
                                              'is_lrg', 'is_nir', 'is_iband',
                                              'is_prisci_vpec_target',
                                              'is_full_vpec_target',
                                              'zspec',
                                              'col_gi', 'col_jk', 'ebv', 'glat',
                                              'mag_j',
                                              'priority',
                                              'visits', 'repeats', 'done',
                                              'difficulty', 'is_active',
                                              'success', 'observations',
                                              'is_sdss_legacy',
                                              'has_sdss_zspec',
                                              'ancillary_flags',
                                              'ancillary_priority',
                                              ])

    logging.info('Extracted %d targets from database' % len(targets_db))
    return targets_db
