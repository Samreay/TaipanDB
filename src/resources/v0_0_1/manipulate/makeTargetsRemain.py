# Compute the targets observed & remaining in individual tiles/fields
# This script/function is designed for bulk insertions into tiling_info;
# for smaller-scale updates, other functions will be required

import logging

from src.scripts.extract import extract_from_joined
from src.scripts.manipulate import update_rows
from taipan.core import TaipanTarget, TaipanTile


def execute(cursor):
    logging.info('Bulk-computing sci targets observed/remaining for tiles')

    # Read-in tiles which do not have n_sci_rem and n_sci_obs defined
    logging.debug('Extracting tiles')
    tile_db = extract_from_joined(cursor, ['field', 'tile', 'tiling_info'],
                                  conditions=[('n_sci_rem', 'IS', 'NULL'),
                                              ('n_sci_obs', 'IS', 'NULL')],
                                  columns=['field_id', 'tile_pk', 'ra', 'dec'])
    # Cut this list down so there is only one tile per field (given this
    # is really a per-field calculation)
    logging.debug('Constructing field tiles')
    fields = list(set([t['field_id'] for t in tile_db]))
    field_db = [(t for t in tile_db if t['field_id'] == field).next() for
                field in fields]
    tile_list = [TaipanTile(x['ra'],
                            x['dec'],
                            field_id=x['field_id']) for
                 x in field_db]

    # Read in all science targets in two lists - those which are 'done', and
    # those which are not
    logging.debug('Extracting science targets from DB')
    sci_rem = extract_from_joined(cursor, ['target', 'science_target'],
                                  conditions=[('done', '=', False)],
                                  columns=['target_id', 'ra', 'dec',
                                           'ux', 'uy', 'uz'])
    sci_rem_targets = [TaipanTarget(x['target_id'], x['ra'], x['dec'],
                                    ucposn=(x['ux'], x['uy'], x['uz'])) for
                       x in sci_rem]
    sci_obs = extract_from_joined(cursor, ['target', 'science_target'],
                                  conditions=[('done', '=', True)],
                                  columns=['target_id', 'ra', 'dec'])
    sci_obs_targets = [TaipanTarget(x['target_id'], x['ra'], x['dec'],
                                    ucposn=(x['ux'], x['uy'], x['uz'])) for
                       x in sci_obs]

    # For each field number, compute the n_sci_obs and n_sci_rem (store in a
    # dict indexed by field ID)
    logging.debug('Generating data to write back to DB')
    n_sci_obs = {}
    n_sci_rem = {}
    for tile in tile_list:
        n_sci_obs[tile.field_id] = len(tile.available_targets(
            sci_obs_targets))
        n_sci_rem[tile.field_id] = len(tile.available_targets(
            sci_rem_targets))

    # Construct a list to write back on a per-tile basis
    write_back = [[tile['tile_pk'],
                   n_sci_obs[tile['field_id']],
                   n_sci_rem[tile['field_id']]] for tile in tile_db]

    # Write the data back
    logging.debug('Write to DB')
    update_rows(cursor, 'tiling_info', write_back,
                columns=['tile_pk', 'n_sci_obs', 'n_sci_rem'])

    logging.info('Generation of sci targets obs/remaining complete!')

    return

