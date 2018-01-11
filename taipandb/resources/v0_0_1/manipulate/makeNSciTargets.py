# Compute the number of each type of targets remaining in each field
# Note this does a full, in-bulk calculation - for smaller changes, it is
# probably more efficient to do direct manipulation

import logging
import numpy as np
from taipan.core import TaipanTarget, dist_points, TILE_RADIUS
from taipandb.scripts.extract import extract_from, extract_from_joined, \
    extract_from_left_joined, count_grouped_from_joined
from taipandb.resources.v0_0_1.readout.readCentroids import execute as rCexec
# from taipandb.resources.v0_0_1.readout.readTileScores import execute as rTSexec
from taipandb.resources.v0_0_1.readout.readTilePK import execute as rTPKexec
from taipandb.resources.v0_0_1.readout.readCentroidsAffected import execute as rCAexec
from taipandb.scripts.manipulate import update_rows_temptable, update_rows

from taipandb.scripts.connection import get_connection

from joblib import Parallel, delayed

def targets_per_field(fields, targets):
    """
    .. note:: Internal helper function

    Take a list of TaipanTiles, a list of TaipanTargets, and work out the
    number of targets on each tile. No exclusion checking.

    Parameters
    ----------
    fields: :obj:`list` of :obj:`taipan.core.TaipanTile`
        List of TaipanTiles to consider. Should only be one tile per field.
    targets: :obj:`list` of :obj:`taipan.core.TaipanTarget`
        List of TaipanTargets to consider.

    Returns
    -------
    output: :obj:`list` of (:obj:`int`, :obj:`int`) tuples
        A list of (field, no_of_targets) tuples.
    """

    output = [(tile.field_id, len(tile.available_targets(targets))) for
              tile in fields]
    return output


def chunks(l, n=100):
    """
    Yield consectuive n-sized chunks of input list l
    """
    for i in range(0, len(l), n):
        yield l[i:i + n]


def multithread_task(fields,
                     cursor,
                     conds_pri_sci=[],
                     cond_combs_pri_sci=[],
                     write_conds=[]):
    """
    .. note:: Internal helper function

    This function is used to parallelise the action of
    :any:`makeNSciTargets.execute`.
    """
    # Need it's own cursor
    cursor_internal = cursor.connection.cursor()

    logging.info('Batch-computing target info for %d fields' % len(fields))

    # Read completed targets
    # We return the *field_id* corresponding to each applicable target, and
    # count these up to form the number written to the database
    targets_complete = extract_from_joined(cursor_internal,
                                           ['target_posn', 'science_target'],
                                           conditions=conds_pri_sci + [
                                               ('done', 'IS NOT', 'NULL'),
                                               ('field_id', 'IN', fields)
                                           ],
                                           conditions_combine=
                                           cond_combs_pri_sci + ['AND'],
                                           columns=['target_id',
                                                    'field_id'],
                                           distinct=True)['field_id']
    # tgt_per_field = []
    # for field in list(set(_[1] for _ in targets_complete)):
    logging.debug(targets_complete)
    if len(targets_complete) > 0:
        logging.debug('Counting targets per field')
        tgt_per_field_comp = [[field, len(targets_complete) -
                               np.count_nonzero(targets_complete - field)]
                              for field in fields]
        logging.debug(tgt_per_field_comp)
        logging.debug('Writing completed target counts to database')
        update_rows(cursor_internal, 'tiling_info', tgt_per_field_comp,
                    columns=['field_id', 'n_sci_obs'],
                    conditions=write_conds)
        # update_rows(cursor_internal, 'tiling_info', tgt_per_field,
        #             columns=['field_id', 'n_sci_obs'])

    tgts_incomplete = extract_from_joined(cursor_internal,
                                          ['science_target', 'target_posn'],
                                          conditions=conds_pri_sci + [
                                              ('done', 'IS', 'NULL'),
                                              ('field_id', 'IN', fields)
                                          ],
                                          conditions_combine=
                                          cond_combs_pri_sci + ['AND'],
                                          columns=['target_id', 'field_id'],
                                          distinct=True)

    # Read targets that are assigned, but yet to be observed
    # Targets must fulfil two criteria:
    # - They can't be marked as done in science_target
    # - The assigned tile must not be marked observed
    # Note that we want the queued tile info from ALL tiles, because targets
    # at the edge of the region of interest may be assigned to tiles outside
    # the ROI
    queued_tile_info = extract_from_joined(cursor_internal,
                                           ['tile', 'target_field',
                                            'science_target'],
                                           conditions=conds_pri_sci + [
                                               ('is_observed', '=', False),
                                               ('is_queued', '=', False),
                                               # ('field_id', 'IN', fields)
                                           ],
                                           conditions_combine=
                                           cond_combs_pri_sci + [
                                               'AND',
                                               # 'AND'
                                           ],
                                           columns=['target_id',
                                                    # 'field_id',
                                                    # 'tile_pk',
                                                    ],
                                           distinct=True,
                                           )

    field_per_tgt = tgts_incomplete[
        np.in1d(tgts_incomplete['target_id'], queued_tile_info['target_id'])
    ]['field_id']

    if len(field_per_tgt) > 0:
        logging.debug('Counting assigned targets per field')
        tgt_per_field_ass = [[field, len(field_per_tgt) -
                              np.count_nonzero(field_per_tgt - field)]
                             for field in fields]
        logging.debug('Writing assigned target counts to database')
        update_rows(cursor_internal, 'tiling_info', tgt_per_field_ass,
                    columns=['field_id', 'n_sci_alloc'],
                    conditions=write_conds)


    # Read targets which are not assigned to any tile yet, nor observed
    # Note that this means we have to find any targets which either:
    # - Have no entries in target_field;
    # - Have entries in target_field, but all the related tiles are set to
    #   'observed' and the target isn't marked as 'done'
    # Can just reverse what we did above!

    # field_per_tgt = tgts_incomplete[
    #     ~np.in1d(tgts_incomplete['target_id'], queued_tile_info['target_id'])
    # ]['field_id']

    # ALGORITHM CHANGE
    # n_sci_rem should now track *all* unobserved targets (allocated or no)
    field_per_tgt = tgts_incomplete['field_id']

    if len(field_per_tgt) > 0:
        logging.debug('Counting remaining targets per field')
        tgt_per_field_nil = [[field, len(field_per_tgt) -
                              np.count_nonzero(field_per_tgt - field)]
                             for field in fields]
    else:
        tgt_per_field_nil = [[field, 0] for field in fields]
    logging.debug('Writing remaining target counts to database')
    update_rows(cursor_internal, 'tiling_info', tgt_per_field_nil,
                columns=['field_id', 'n_sci_rem'],
                conditions=write_conds)

    # Count the number of science targets marked with 'success' in the field
    # Don't worry about what they are
    done_target_count = count_grouped_from_joined(cursor_internal,
                                                  ['target_posn',
                                                   'science_target'],
                                                  'field_id',
                                                  conditions=[
                                                      ('success', '=', True),
                                                      ('field_id', 'IN',
                                                       fields),
                                                  ])
    if len(done_target_count) > 0:
        done_target_count = [[_['field_id'], _['count']] for _ in
                             done_target_count]
        update_rows(cursor_internal, 'tiling_info', done_target_count,
                    columns=['field_id', 'n_done'],
                    conditions=write_conds)

    cursor_internal.connection.commit()
    cursor_internal.close()
    return


def execute(cursor, fields=None, use_pri_sci=True,
            unobserved_only=True,
            multicores=7, chunk_size=1000):
    """
    Calculate the number of targets in each field of each status type.

    Parameters
    ----------
    cursor: :obj:`psycopg2.connection.cursor`
        psycopg2 cursor for communicating with the database.
    fields: :obj:`list` of :obj:`int`
        A list of IDs of the fields that need updating. An error will be thrown
        if the fields listed here cannot be found in the database. Defaults to
        None, at which point all fields will be updated.
        Note that this should be a list of the fields where you know changes
        have occurred. The function will automatically add adjacent, overlapping
        fields to the query.
    use_pri_sci: :obj:`bool`
        Optional Boolean, determining whether target numbers should be computed
        from all targets in the database (False), or only those attached to
        a primary science case (i.e. have at least one of is_h0_target,
        is_vpec_target or is_lowz_target set to True). Defaults to True.
    unobserved_only : :obj:`bool`
        Whether to write tile scores only against tiles where that haven't
        been observed yet (True), or against all tiles of that field (False).
        Defaults to True.
    multicores: :obj:`int`
        Number of cores to use (i.e. values > 1 activate multi-threaded
        processing). Defaults to 4.
    chunk_size: :obj:`int`
        The number of fields to calculate target information for
        simultaneously as part of multi-threading. Defaults to 1000.

    Returns
    -------
    :obj:`None`
        The database is updated in place. The function looks at the
        contents of the ``science_target`` table, and places targets into three
        categories:

        - Remaining (i.e observing is not complete)
        - Allocated (i.e. assigned to a tile, but observing isn't complete)
        - Observed (i.e. observing that target is done)

        It then writes these values to the relevant columns
        (``n_sci_rem``, ``n_sci_alloc``,
        ``n_sci_obs``) of the ``tiling_info`` table.
    """

    logging.info('Doing a bulk calculation of per-field target statuses '
                 'using makeNSciTargets')

    if multicores < 1:
        raise ValueError('multicores must be >= 1')

    # Unfortunately, we can't just use the assigned tile/field to increment
    # the target count for that tile/field - targets can and will appear in
    # multiple fields

    # Read in the fields information
    # We need to read *all* fields so we can find the overlaps
    field_tiles = rCexec(cursor)

    conds_pri_sci = []
    cond_combs_pri_sci = []
    if use_pri_sci:
        # conds_pri_sci = [
        #     ('(', 'is_h0_target', '=', True, ''),
        #     ('', 'is_vpec_target', '=', True, ''),
        #     ('', 'is_lowz_target', '=', True, ')'),
        # ]
        # cond_combs_pri_sci = ['OR', 'OR', 'AND']
        conds_pri_sci = [
            ('priority', '>', 49)
        ]
        cond_combs_pri_sci = ['AND']

    write_conds = []
    if unobserved_only:
        unobs_tiles = list(rTPKexec(cursor,
                                    conditions=[
                                        ('is_queued', '=', False),
                                        ('is_observed', '=', False),
                                    ]))
        if len(unobs_tiles) > 0:
            write_conds += [
                ('tile_pk', 'IN', unobs_tiles),
            ]
        else:
            # No tile to write against - abort
            return

    # The second part of this if test occurs when we're needing to do the
    # computation for all fields, but have (for some reason) specified the full
    # field list
    if fields is None or len(fields) == len(field_tiles):
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
        # fields = list(set([f.field_id for f in field_tiles if
        #                    np.any([dist_points(f.ra, f.dec,
        #                                        field.ra,
        #                                        field.dec) < 2*TILE_RADIUS for
        #                            field in
        #                            [x for x in field_tiles if
        #                             x.field_id in fields]])]))
        fields = rCAexec(cursor, field_list=fields)
        logging.debug('Fields to be looked at (total %d): %s' %
                      (len(fields), ', '.join(str(f) for f in fields), ))

    _ = Parallel(n_jobs=multicores, backend='threading')(
        delayed(multithread_task)(fields[i:i+chunk_size], cursor,
                                  conds_pri_sci=conds_pri_sci,
                                  cond_combs_pri_sci=cond_combs_pri_sci,
                                  write_conds=write_conds)
        for i in range(0, len(fields), chunk_size)
    )

    return


    # ------
    # ORIGINAL SINGLE-THREAD IMPLEMENTATION
    # MAINTAINED FOR SAFETY
    # ------
    # Read completed targets
    # We return the *field_id* corresponding to each applicable target, and
    # count these up to form the number written to the database
    targets_complete = extract_from_joined(cursor,
                                           ['target_posn', 'science_target'],
                                           conditions=conds_pri_sci + [
                                               ('done', 'IS NOT', 'NULL'),
                                               ('field_id', 'IN', fields)
                                           ],
                                           conditions_combine=
                                           cond_combs_pri_sci + ['AND'],
                                           columns=['target_id',
                                                    'field_id'],
                                           distinct=True)['field_id']
    # tgt_per_field = []
    # for field in list(set(_[1] for _ in targets_complete)):
    logging.debug(targets_complete)
    if len(targets_complete) > 0:
        logging.debug('Counting targets per field')
        tgt_per_field_comp = [[field, len(targets_complete) -
                               np.count_nonzero(targets_complete - field)]
                              for field in fields]
        logging.debug(tgt_per_field_comp)
        logging.debug('Writing completed target counts to database')
        update_rows(cursor, 'tiling_info', tgt_per_field_comp,
                    columns=['field_id', 'n_sci_obs'],
                    conditions=write_conds)
        # update_rows(cursor, 'tiling_info', tgt_per_field,
        #             columns=['field_id', 'n_sci_obs'])

    tgts_incomplete = extract_from_joined(cursor,
                                          ['science_target', 'target_posn'],
                                          conditions=conds_pri_sci + [
                                              ('done', 'IS', 'NULL'),
                                              ('field_id', 'IN', fields)
                                          ],
                                          conditions_combine=
                                          cond_combs_pri_sci + ['AND'],
                                          columns=['target_id', 'field_id'],
                                          distinct=True)

    # Read targets that are assigned, but yet to be observed
    # Targets must fulfil two criteria:
    # - They can't be marked as done in science_target
    # - The assigned tile must not be marked observed
    # Note that we want the queued tile info from ALL tiles, because targets
    # at the edge of the region of interest may be assigned to tiles outside
    # the ROI
    queued_tile_info = extract_from_joined(cursor,
                                           ['tile', 'target_field',
                                            'science_target'],
                                           conditions=conds_pri_sci + [
                                               ('is_observed', '=', False),
                                               ('is_queued', '=', False),
                                               # ('field_id', 'IN', fields)
                                           ],
                                           conditions_combine=
                                           cond_combs_pri_sci + [
                                               'AND',
                                               # 'AND'
                                           ],
                                           columns=['target_id',
                                                    # 'field_id',
                                                    # 'tile_pk',
                                                    ],
                                           distinct=True,
                                           )

    field_per_tgt = tgts_incomplete[
        np.in1d(tgts_incomplete['target_id'], queued_tile_info['target_id'])
    ]['field_id']

    if len(field_per_tgt) > 0:
        logging.debug('Counting assigned targets per field')
        tgt_per_field_ass = [[field, len(field_per_tgt) -
                              np.count_nonzero(field_per_tgt - field)]
                             for field in fields]
        logging.debug('Writing assigned target counts to database')
        update_rows(cursor, 'tiling_info', tgt_per_field_ass,
                    columns=['field_id', 'n_sci_alloc'],
                    conditions=write_conds)

    # Read targets which are not assigned to any tile yet, nor observed
    # Note that this means we have to find any targets which either:
    # - Have no entries in target_field;
    # - Have entries in target_field, but all the related tiles are set to
    #   'observed' and the target isn't marked as 'done'
    # Can just reverse what we did above!

    # field_per_tgt = tgts_incomplete[
    #     ~np.in1d(tgts_incomplete['target_id'], queued_tile_info['target_id'])
    # ]['field_id']

    # ALGORITHM CHANGE
    # n_sci_rem should now track *all* unobserved targets (allocated or no)
    field_per_tgt = tgts_incomplete['field_id']

    if len(field_per_tgt) > 0:
        logging.debug('Counting remaining targets per field')
        tgt_per_field_nil = [[field, len(field_per_tgt) -
                              np.count_nonzero(field_per_tgt - field)]
                             for field in fields]
        logging.debug('Writing remaining target counts to database')
        update_rows(cursor, 'tiling_info', tgt_per_field_nil,
                    columns=['field_id', 'n_sci_rem'],
                    conditions=write_conds)

    # Count the number of science targets marked with 'success' in the field
    # Don't worry about what they are
    done_target_count = count_grouped_from_joined(cursor,
                                                  ['target_posn',
                                                   'science_target'],
                                                  'field_id',
                                                  conditions=[
                                                      ('success', '=', True),
                                                      ('field_id', 'IN',
                                                       fields),
                                                  ])
    done_target_count = [[_['field_id'], _['count']] for _ in done_target_count]
    if len(fields) < 1000:
        update_rows(cursor, 'tiling_info', done_target_count,
                    columns=['field_id', 'n_done'],
                    conditions=write_conds)
    else:
        update_rows_temptable(cursor, 'tiling_info', done_target_count,
                              columns=['field_id', 'n_done'],
                              conditions=write_conds)

    return
