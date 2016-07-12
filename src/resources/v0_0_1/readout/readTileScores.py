import logging
from ....scripts.extract import execute_select, extract_from_joined, get_columns

import numpy as np


def execute(cursor, metrics=Non):
    """
    Read in the tile 'scores' for tiles awaiting observation

    Parameters
    ----------
    cursor:
        psycopg2 cursor for interacting with the database
    metrics:
        A list of metrics to return, corresponding to the columns of
        the tiling_info table to send back.
        Defaults to None, at which point all metrics will be sent back.

    Returns
    -------

    """

    if cursor is None:
        raise RuntimeError('readTileScores requires a cursor to be specified')
    if metrics is not None and not isinstance(metrics, list):
        raise ValueError('metrics must be a list of metric names')
    if metrics is not None and not np.all([isinstance(m, str) for
                                           m in metrics]):
        raise ValueError('metric must be a list of metric names (i.e. strings)')

    # Get the metric names if the list of passed metrics is None
    if metrics is None:
        table_columns, dtypes = get_columns(cursor, 'tiling_info')
        metrics = [t for t in table_columns if
                   t != 'field_id' or
                   t != 'tile_pk']

    # Check each of the metrics in turn
    # If the metric is None everywhere, exclude it from being returned
    to_pop = []
    for i in range(len(metrics)):
        col_vals = execute_select(cursor.connection,
                                  'SELECT %s FROM tiling_info' %
                                  (metrics[i], ))
        if np.all([c[0] is None for c in col_vals]):
            to_pop.append(i)

    for i in to_pop[::-1]:
        burn = metrics.pop(i)

    # Fetch the metrics from the tiling info table
    return extract_from_joined(cursor, ['field', 'tile', 'tiling_info', ],
                               conditions=[
                                   ('is_observed', '=', False),
                               ],
                               columns=
                               ['tile_pk', 'field_id', 'ra', 'dec'] + metrics)
