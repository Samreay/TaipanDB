# Compute the number of each type of targets remaining in each field
# Note this does a full, in-bulk calculation - for smaller changes, it is
# probably more efficient to do direct manipulation

import logging
import numpy as np
from taipan.core import TaipanTarget, dist_points, TILE_RADIUS
from ....scripts.extract import extract_from_joined, extract_from_left_joined
from ..readout.readCentroids import execute as rCexec
from ....scripts.manipulate import update_rows, update_rows


def targets_per_field(fields, targets):
    """
    INTERNAL HELPER FUNCTION
    Take a list of TaipanTiles, a list of TaipanTargets, and work out the
    number of targets on each tile. No exclusion checking.

    Parameters
    ----------
    fields:
        List of TaipanTiles to consider. Should only be one tile per field.
    targets:
        List of TaipanTargets to consider.

    Returns
    -------
    output:
        A list of (field, no_of_targets) tuples.
    """

    output = [(tile.field_id, len(tile.available_targets(targets))) for
              tile in fields]
    return output


def execute(cursor, fields=None, use_pri_sci=True):
    """
    Calculate the number of targets in each field of each status type.

    Parameters
    ----------
    cursor:
        psycopg2 cursor for communicating with the database.
    fields:
        A list of IDs of the fields that need updating. An error will be thrown
        if the fields listed here cannot be found in the database. Defaults to
        None, at which point all fields will be updated.
        Note that this should be a list of the fields where you know changes
        have occurred. The function will automatically add adjacent, overlapping
        fields to the query.
    use_pri_sci:
        Optional Boolean, determining whether target numbers should be computed
        from all targets in the database (False), or only those attached to
        a primary science case (i.e. have at leastone of is_h0_target,
        is_vpec_target or is_lowz_target set to True). Defaults to True

    Returns
    -------
    Nil - the database is updated in place. The function looks at the
    contents of the science_target table, and places targets into three
    categories:
    - Remaining (i.e observing is not complete)
    - Allocated (i.e. assigned to a tile, but observing isn't complete)
    - Observed (i.e. observing that target is done)
    It then writes these values to the relevant columns (n_sci_rem, n_sci_alloc,
    n_sci_obs) of the tiling_info table.
    """

    logging.info('Doing a bulk calculation of per-field target statuses '
                 'using makeNSciTargets')

    # Unfortunately, we can't just use the assigned tile/field to increment
    # the target count for that tile/field - targets can and will appear in
    # multiple fields

    # Read in the fields information
    # We need to read *all* fields so we can find the overlaps
    field_tiles = rCexec(cursor)

    conds_pri_sci = []
    cond_combs_pri_sci = []
    if use_pri_sci:
        conds_pri_sci = [
            ('(', 'is_h0_target', '=', True, ''),
            ('', 'is_vpec_target', '=', True, ''),
            ('', 'is_lowz_target', '=', True, ')'),
        ]
        cond_combs_pri_sci = ['OR', 'OR', 'AND']

    if fields is None:
        fields = [field.field_id for field in field_tiles]
    else:
        # Check to see if all the requested fields are present
        try:
            if len(fields) != len([field for field in fields if
                                   field in [x.field_id for x in field_tiles]]):
                raise ValueError('One of the requested fields could not be '
                                 'found in the database. Please check your '
                                 'inputs.')
        except TypeError:
            raise ValueError('fields must be a list (or iterable) of field IDs')
        # Expand the list of fields to include any which overlap with those
        # given as inputs
        logging.debug('Requested fields (total %d): %s' %
                      (len(fields), ', '.join(str(f) for f in fields), ))
        fields = list(set([f.field_id for f in field_tiles if
                           np.any([dist_points(f.ra, f.dec,
                                               field.ra,
                                               field.dec) < 2*TILE_RADIUS for
                                   field in
                                   [x for x in field_tiles if
                                    x.field_id in fields]])]))
        logging.debug('Fields to be looked at (total %d): %s' %
                      (len(fields), ', '.join(str(f) for f in fields), ))

    # Read completed targets
    targets_complete = np.asarray(
        extract_from_joined(cursor,
                            ['target_posn', 'science_target'],
                            conditions=conds_pri_sci + [
                                ('done', '=', True),
                                ('field_id', 'IN', fields)
                            ],
                            conditions_combine=cond_combs_pri_sci + ['AND'],
                            columns=['field_id']),
        dtype=int)
    # tgt_per_field = []
    # for field in list(set(_[1] for _ in targets_complete)):
    if len(targets_complete) > 0:
        logging.debug('Counting targets per field')
        tgt_per_field = [[field, len(targets_complete) -
                          np.count_nonzero(targets_complete - field)]
                         for field in fields]
        logging.debug(tgt_per_field)
        logging.debug('Writing target counts to database')
        update_rows(cursor, 'tiling_info', tgt_per_field,
                    columns=['field_id', 'n_sci_obs'])
        update_rows(cursor, 'tiling_info', tgt_per_field,
                    columns=['field_id', 'n_sci_obs'])

    # Read targets that are assigned, but yet to be observed
    # Targets must fulfil two criteria:
    # - They can't be marked as done in science_target
    # - The assigned tile must not be marked observed
    logging.debug('Extracting assigned targets...')
    targets_assigned = np.asarray(extract_from_joined(cursor,
                                                      ['target_posn',
                                                       'science_target',
                                                       'target_field', 'tile'],
                                                      conditions=
                                                      conds_pri_sci + [
                                                          ('done', '=', False),
                                                          ('is_observed', '=',
                                                           False),
                                                      ],
                                                      conditions_combine=
                                                      cond_combs_pri_sci + [
                                                          'AND'
                                                      ],
                                                      columns=['field_id']),
                                  dtype=int)
    if len(targets_assigned) > 0:
        logging.debug('Counting targets per field')
        tgt_per_field = [[field, len(targets_assigned) -
                          np.count_nonzero(targets_assigned - field)]
                         for field in fields]
        logging.debug('Writing target counts to database...')
        update_rows(cursor, 'tiling_info', tgt_per_field,
                    columns=['field_id', 'n_sci_alloc'])

    # Read targets which are not assigned to any tile yet, nor observed
    # Note that this means we have to find any targets which either:
    # - Have no entries in target_field;
    # - Have entries in target_field, but all the related tiles are set to
    #   'observed' and the target isn't marked as 'done'
    logging.debug('Extracting currently unassigned targets...')
    target_stats_array_a = np.asarray(
        extract_from_joined(
            cursor,
            ['target_posn', 'science_target', 'target_field', 'tile'],
            conditions=conds_pri_sci + [
                ('done', '=', False),
                ('is_observed', '=', True),
            ],
            conditions_combine=cond_combs_pri_sci + ['AND'],
            columns=['field_id'],
            distinct=True),
        dtype=int)
    logging.debug('Type a shape: %s' % str(target_stats_array_a.shape))
    target_stats_array_b = np.asarray(
        extract_from_left_joined(
            cursor,
            ['target_posn', 'science_target', 'target_field'],
            'target_id',
            conditions=conds_pri_sci + [
                # ('is_science', '=', True),
                ('done', '=', False),
                ('tile_pk', 'IS', 'NULL'),
            ],
            conditions_combine=cond_combs_pri_sci + ['AND'],
            columns=['field_id']),
        dtype=int)
    logging.debug('Type b shape: %s' % str(target_stats_array_b.shape))
    target_stats_array = np.concatenate((target_stats_array_a,
                                         target_stats_array_b))
    # tgt_per_field = []
    if len(target_stats_array) > 0:
        logging.debug('Counting targets per field')
        tgt_per_field = [[field, len(target_stats_array) -
                          np.count_nonzero(target_stats_array - field)]
                         for field in fields]
        logging.debug(tgt_per_field)
        logging.debug('Writing target counts to database')
        update_rows(cursor, 'tiling_info', tgt_per_field,
                    columns=['field_id', 'n_sci_rem'])

    return
