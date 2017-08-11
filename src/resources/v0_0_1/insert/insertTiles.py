# Insert unpicked tiles into the database

import logging
from taipan.core import TaipanTile
from ....scripts.create import insert_many_rows, create_index
from ....scripts.extract import extract_from, extract_from_joined
from ..manipulate.makeNSciTargets import execute as mNScTexec
from ...v0_0_1 import SKY_TARGET_ID
import numpy as np

import datetime

from ..readout import readScience as rSc


# def compute_sci_targets_complete(cursor, tile, tgt_list):
#     """
#     Compute the number of science targets remaining in a particular field
#
#     Parameters
#     ----------
#     tile
#     tgt_list
#
#     Returns
#     -------
#
#     """
#
#     # Get the list of targets
#     query_result = rSc.execute(cursor)


def execute(cursor, tile_list, is_queued=False, is_observed=False,
            config_time=datetime.datetime.now(), disqualify_below_min=True,
            remove_index=True):
    """
    Insert the given tiles into the database.

    Parameters
    ----------
    cursor:
        psycopg2 cursor for interacting with the database
    tile_list:
        A list of TaipanTile objects to write into the database
    is_queued:
        Optional; whether the passed tiles should be considered queued or not.
        Defaults to False.
    is_observed:
        Optional; whether the passed tiles should be considered observed.
        Defaults to False.
    config_time:
        Optional; the datetime (timestamp) when the tile was configured.
        Defaults to datetime.datetime.now().
    disqualify_below_min:
        Optional Boolean, passed on through to
        TaipanTile.calculate_tile_score(). Sets tile scores to 0 if tiles do
        not meet minimum numbers of guides and/or standards assigned. Defaults
        to True.
    remove_index: Boolean (default: True)
        Boolean value denoting whether to strip the tile_pk index off
        the target_field table before data insertion, and regenerate
        it afterwards. Provides a performance boost when inserting a large-ish
        number of tiles. Defaults to True.

    Returns
    -------
    Nil. Tiles are pushed into the database.
    """
    logging.info('Inserting tiles into database...')

    if not tile_list or len(tile_list) == 0:
        logging.info('No list of tiles passed - aborting insert')
        return

    # Deprecated first pass - assume we are writing first tile for each field
    # write_to_tile = [[1, t.field_id, is_queued, is_observed]
    #                  for t in tile_list]

    # Read out the already existing tile_id
    logging.info('--- Fetching existing tile IDs...')
    logging.info('  --- Read in...')
    tile_ids = extract_from(cursor, 'tile',
                            columns=['field_id', 'tile_id'])
    logging.info('  --- Forming field set...')
    fields = list(set([t['field_id'] for t in tile_ids]))
    logging.info('  --- Determine max existing tile id...')
    # fields = np.asarray(fields)
    # tile_id_max = {field: max([t['tile_id'] for t in tile_ids if
    #                            t['field_id'] == field]) | 0 for
    #                field in fields}
    tile_id_max = {field: np.max(tile_ids[tile_ids['field_id'] ==
                                          field]['tile_id']) | 0 for
                   field in fields}
    # Now, include fields into the mix that don't have DB entries yet
    logging.info('  --- Include missing fields...')
    for field in [t.field_id for t in tile_list]:
        try:
            _ = tile_id_max[field]
        except KeyError:
            tile_id_max[field] = 0

    # Create the data to write to the DB
    logging.info('--- Forming data to write to DB...')
    write_to_tile = []
    field_tile_id_pairs = []
    for t in tile_list:
        write_to_tile.append([tile_id_max[t.field_id] + 1,
                              t.field_id, is_queued, is_observed])
        field_tile_id_pairs.append((tile_id_max[t.field_id] + 1,
                                    t.field_id))
        tile_id_max[t.field_id] += 1

    columns_to_tile = ['tile_id', 'field_id', 'is_queued', 'is_observed']

    # Write to the tile table
    if cursor is not None:
        logging.info('--- Writing to "tile" table...')
        insert_many_rows(cursor, "tile", write_to_tile,
                         columns=columns_to_tile)

    # Beyond this point, we need to read back in the increment primary
    # keys that the database auto-generated, so quit here if cursor is None
    if cursor is None:
        logging.info('No cursor provided - aborting insert')
        return

    # Read back the primary keys of the tiles we just created
    # UPDATES 20170718 - this needs to be split into chunks of 2000 to avoid
    # overloading the max_stack_depth of the server
    c = 0
    bite_size = 2000
    pk_dict = {}
    while c < len(field_tile_id_pairs):
        query_result = extract_from(cursor, 'tile',
                                    conditions=[('(tile_id,field_id)',
                                                 'IN',
                                                 '(%s)' %
                                                 (','.join([str(p) for
                                                            p in
                                                            field_tile_id_pairs[
                                                            c:c+bite_size]]),
                                                  ))],
                                    columns=['tile_pk', 'field_id', 'tile_id'])
        # Re-format to something more useful
        pk_dict_chunk = {q[1]: q[0] for q in query_result}
        pk_dict.update(pk_dict_chunk)
        c += bite_size

    # Back-fill the tile_list with the newly-assigned PKs
    for tile in tile_list:
        tile.pk = pk_dict[tile.field_id]

    # Construct the list of target field assignments to write to database
    target_assigns = []
    for t in tile_list:
        # logging.debug('Tile list:')
        # logging.debug(t.fibres)
        target_assign = [[t.fibres[f].idn, f, pk_dict[t.field_id]]
                         for f in t.fibres
                         if t.fibres[f] is not None and
                         not isinstance(t.fibres[f], str)]
        target_sky = [[SKY_TARGET_ID, f, pk_dict[t.field_id]] for f in t.fibres
                      if t.fibres[f] == 'sky']
        target_assigns += target_assign
        target_assigns += target_sky

    columns_to_target_field = ['target_id', 'bug_id', 'tile_pk']

    # Write the target assignments to DB
    logging.info('--- Writing to "target_field" table...')
    if remove_index:
        cursor.execute('DROP INDEX IF EXISTS target_field_tile_pk_idx')
    insert_many_rows(cursor, "target_field", target_assigns,
                     columns=columns_to_target_field)
    if remove_index:
        create_index(cursor, 'target_field', ['tile_pk', ])

    logging.debug('Adding tile score info to tiling_info table')
    logging.debug('Computing tile scores')
    columns_scores = ['tile_pk',
                      'field_id',
                      'n_sci_alloc',
                      'diff_sum',
                      # 'diff_prod',
                      'prior_sum',
                      # 'prior_prod',
                      'cw_sum',
                      # 'cw_prod',
                      ]
    tiling_scores = [[t.pk,
                      t.field_id,
                      t.calculate_tile_score(method='completeness',
                                             disqualify_below_min=
                                             disqualify_below_min),
                      t.calculate_tile_score(method='difficulty-sum',
                                             disqualify_below_min=
                                             disqualify_below_min),
                      # t.calculate_tile_score(method='difficulty-prod',
                      #                        disqualify_below_min=
                      #                        disqualify_below_min),
                      t.calculate_tile_score(method='priority-sum',
                                             disqualify_below_min=
                                             disqualify_below_min),
                      # t.calculate_tile_score(method='priority-prod',,
                      #                        disqualify_below_min=
                      #                        disqualify_below_min),
                      t.calculate_tile_score(method='combined-weighted-sum',
                                             disqualify_below_min=
                                             disqualify_below_min),
                      # t.calculate_tile_score(method='combined-weighted-prod',
                      #                        disqualify_below_min=
                      #                        disqualify_below_min),
                      ]
                     for t in tile_list]
    logging.debug(tiling_scores)
    if len(tiling_scores[0]) != len(columns_scores):
        raise RuntimeError('There is a programming error in insertTiles, '
                           'where tiling scores are computed - please check')

    logging.info('--- Writing tile scores to DB')
    insert_many_rows(cursor, 'tiling_info', tiling_scores,
                     columns=columns_scores)

    logging.debug('Now going to call makeNSciTargets to calculate the '
                  'remaining scores')
    mNScTexec(cursor, fields=list(set(t.field_id for t in tile_list)))

    # Finally, let's insert the config date into the tiling_config table
    logging.debug('Insert configuration time into database')
    insert_many_rows(cursor, 'tiling_config',
                     [[tile.pk, config_time] for tile in tile_list],
                     columns=['tile_pk', 'date_config'])

    logging.info('Inserting tiles complete!')
    return
