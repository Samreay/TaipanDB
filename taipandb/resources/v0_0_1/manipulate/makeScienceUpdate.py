# This is a 'super' method, designed to update all shifting target
# information (priority, difficulty, types) in a single pass.
# The aim is to limit the number of read/writes to the DB as much as
# possible, as this is the primary performance penalty
# To do this on a per-field basis, you'll need to compute the target
# list yourself

import logging
from ....scripts.extract import extract_from_joined
from ....scripts.manipulate import increment_rows, update_rows_temptable

import datetime

import numpy as np


def execute(cursor, target_info_array,
            block_size=100000):
    """
    Update target priorities & difficulties in the database.

    Parameters
    ----------
    cursor: :obj:`psycopg2.connection.cursor`
        psycopg2 cursor for communicating with the database.
    target_info_array : :obj:`numpy.array`
        NumPy structured array holding the target information. Can really
        be any structured array, so long as it has the following columns
        (extra columns will have no effect):

        - `target_id`
        - `priority`
        - `difficulty`
        - `is_h0_target`
        - `is_vpec_target`
        - `is_lowz_target`

    Returns
    -------
    :obj:`None`
        Types are written into the database

    """

    logging.info('Updating target diffs/priorities/types in database...')
    start = datetime.datetime.now()

    data = [(
        target_info_array['target_id'][i],
        target_info_array['difficulty'][i],
        target_info_array['priority'][i],
        bool(target_info_array['is_h0_target'][i]),
        bool(target_info_array['is_vpec_target'][i]),
        bool(target_info_array['is_lowz_target'][i]),
    ) for i in range(len(target_info_array))]

    # update_rows_temptable(cursor, 'science_target',
    #                       np.transpose(np.asarray(
    #                           [list(target_info_array['target_id']),
    #                            list(target_info_array['difficulty']),
    #                            list(target_info_array['priority']),
    #                            list(target_info_array['is_h0_target']),
    #                            list(target_info_array['is_vpec_target']),
    #                            list(target_info_array['is_lowz_target']),
    #                            ])), columns=['target_id',
    #                                          'difficulty',
    #                                          'priority',
    #                                          'is_h0_target',
    #                                          'is_vpec_target',
    #                                          'is_lowz_target'])

    i = 0
    while i < len(data):
        update_rows_temptable(cursor, 'science_target',
                              data[i:i+block_size],
                              columns=['target_id',
                                       'difficulty',
                                       'priority',
                                       'is_h0_target',
                                       'is_vpec_target',
                                       'is_lowz_target'])
        i += block_size

    end = datetime.datetime.now()
    delta = (end - start).total_seconds()
    logging.info('Done!')
    logging.debug('(in %4.1fs)' % delta)

    return
