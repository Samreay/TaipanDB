# Insert unpicked tiles into the database

import logging
from taipan.core import TaipanTile
from ....scripts.create import insert_many_rows
from ....scripts.extract import extract_from, extract_from_joined
from ...v0_0_1 import SKY_TARGET_ID

from ..readout import readScience as rSc


def compute_sci_targets_complete(cursor, tile, tgt_list):
    """
    Compute the number of science targets remaining in a particular field

    Parameters
    ----------
    tile
    tgt_list

    Returns
    -------

    """

    # Get the list of targets
    query_result = rSc.execute(cursor)


def execute(cursor, tile_list, is_queued=False, is_observed=False):
    logging.info('Inserting tiles into database...')

    if not tile_list:
        logging.info('No list of tiles passed - aborting insert')
        return

    # Deprecated first pass - assume we are writing first tile for each field
    # write_to_tile = [[1, t.field_id, is_queued, is_observed]
    #                  for t in tile_list]

    # Read out the already existing tile_id
    tile_ids = extract_from(cursor, 'tile',
                            columns=['field_id', 'tile_id'])
    fields = list(set([t['field_id'] for t in tile_ids]))
    tile_id_max = {field : max([t['tile_id'] for t in tile_ids if
                                t['field_id'] == field]) | 0 for
                   field in fields}
    # Now, include fields into the mix that don't have DB entries yet
    for field in [t.field_id for t in tile_list]:
        try:
            _ = tile_id_max[field]
        except KeyError:
            tile_id_max[field] = 0

    # Create the data to write to the DB
    write_to_tile = []
    for t in tile_list:
        write_to_tile.append([tile_id_max[t.field_id] + 1 | 0,
                              t.field_id, is_queued, is_observed])
        tile_id_max[t.field_id] += 1

    columns_to_tile = ['tile_id', 'field_id', 'is_queued', 'is_observed']

    # Write to the tile table
    if cursor is not None:
        logging.debug('Writing to "tile" table...')
        insert_many_rows(cursor, "tile", write_to_tile,
                         columns=columns_to_tile)

    # Beyond this point, we need to read back in the increment primary
    # keys that the database auto-generated, so quit here if cursor is None
    if cursor is None:
        logging.info('No cursor provided - aborting insert')
        return

    # Read back the primary keys of the tiles we just created
    query_result = extract_from(cursor, 'tile',
                                conditions=[('(tile_id,field_id)',
                                             'IN',
                                             '(%s)' %
                                             (','.join([str((1, t.field_id))
                                                        for t in tile_list]),
                                              ))],
                                columns=['tile_pk', 'field_id', 'tile_id'])
    # Re-format to something more useful
    pk_dict = {q[1]: q[0] for q in query_result}
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
    logging.debug('Writing to "target_field" table...')
    insert_many_rows(cursor, "target_field", target_assigns,
                     columns=columns_to_target_field)

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
                      t.calculate_tile_score(method='completeness'),
                      t.calculate_tile_score(method='difficulty-sum'),
                      # t.calculate_tile_score(method='difficulty-prod'),
                      t.calculate_tile_score(method='priority-sum'),
                      # t.calculate_tile_score(method='priority-prod'),
                      t.calculate_tile_score(method='combined-weighted-sum'),
                      # t.calculate_tile_score(method='combined-weighted-prod'),
                      ]
                     for t in tile_list]
    logging.debug(tiling_scores)
    if len(tiling_scores[0]) != len(columns_scores):
        raise RuntimeError('There is a programming error in insertTiles, '
                           'where tiling scores are computed - please check')

    logging.debug('Writing tile scores to DB')
    insert_many_rows(cursor, 'tiling_info', tiling_scores,
                     columns=columns_scores)

    logging.info('Inserting tiles complete!')
    return
