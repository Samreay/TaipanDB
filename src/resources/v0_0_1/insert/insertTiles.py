# Insert unpicked tiles into the database

import logging
from taipan.core import TaipanTile
from ....scripts.create import insert_many_rows
from ....scripts.extract import extract_from


def execute(cursor, tile_list, is_queued=False, is_observed=False):
    logging.info('Inserting tiles into database...')

    if not tile_list:
        logging.info('No list of tiles passed - aborting insert')
        return

    # First pass - assume we are just writing the first tile for
    # each field
    # TODO: Expand to allow arbitraty tile_id - this will required tile_id
    # TODO: in the tiler/simulator
    write_to_tile = [[1, t.field_id, is_queued, is_observed]
                     for t in tile_list]
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

    # Construct the list of target field assignments to write to database
    target_assigns = []
    for t in tile_list:
        target_assign = [[t.fibres[f].idn, f, pk_dict[t.field_id]]
                         for f in t.fibres
                         if t.fibres[f] is not None and t.fibres[f] != 'sky']
        target_blank = [[None, f, pk_dict[t.field_id]] for f in t.fibres
                        if t.fibres[f] is None]
        target_assigns += target_assign
        target_assigns += target_blank

    columns_to_target_field = ['target_id', 'bug_id', 'tile_pk']

    # Write the target assignments to DB
    if cursor is not None:
        logging.debug('Writing to "target_field" table...')
        insert_many_rows(cursor, "target_field", target_assigns,
                         columns=columns_to_target_field)

    logging.info('Inserting tiles complete!')
    return
