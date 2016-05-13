import logging
from ....scripts.extract import extract_from, extract_from_joined
from taipan.core import TaipanTile

import readScience as rS


def execute(cursor, target_list=None):
    logging.info('Reading tiles from database')

    if target_list is None:
        # Will need to read the targets in from the database so we have
        # something to build the tiles from
        return_target_list = True
        target_list = rS.execute(cursor)

    # Get the fibre assignments
    fibreassigns = extract_from_joined(cursor,
                                       ['tile', 'target_field'],
                                       conditions=None,
                                       columns=['tile_pk',
                                                'field_id',
                                                'bug_id',
                                                'target_id'])
    # Get the position information for the fields
    fieldposns = extract_from(cursor,
                              'field',
                              conditions=None,
                              columns=['field_id', 'ra', 'dec'])

    tile_list = []

    # Re-construct the tiles from the list we got from the database
    # Construct a list of tile_pks present

    pks = list(set([row['tile_pk'] for row in fibreassigns]))
    for pk in pks:
        # TODO: Get the field ID (what's an efficient means?)
        # TODO: Get the field RA and DEC from fieldposns
        # Create the tile
        new_tile = TaipanTile(...)
        # TODO: Assign TaipanTarget objects from target_list to the tile
        # TODO: Any bugs not listed in the database are 'sky' - set
        #       these as such

    if return_target_list:
        return tile_list, target_list
    return tile_list
