import logging
from ....scripts.extract import extract_from, extract_from_joined
from taipan.core import TaipanTile, BUGPOS_OFFSET
from ...v0_0_1 import SKY_TARGET_ID

import readScience as rS
import readGuides as rG
import readStandards as rSt

from bisect import bisect_left


def index(a, x):
    """
    Bisect based method for efficient searching in ordered lists
    Parameters
    ----------
    a - Input list
    x - Value to search for

    Returns
    -------
    i - The index of the left-most value in a exactly equal to x
    """
    i = bisect_left(a, x)
    if i != len(a) and a[i] == x:
        return i
    raise ValueError('Value x is not in list a!')


def execute(cursor, candidate_targets=None, guide_targets=None,
            standard_targets=None):

    return_targets = False

    if cursor is None and (candidate_targets is None or
                           guide_targets is None or
                           standard_targets is None):
        raise RuntimeError('If using readTiles without a cursor, '
                           'you must provide all three target lists')

    logging.info('Reading tiles from database')

    if candidate_targets is None:
        # Will need to read the targets in from the database so we have
        # something to build the tiles from
        logging.debug('Reading in list of science targets from DB')
        return_targets = True
        candidate_targets = rS.execute(cursor)

    if guide_targets is None:
        # Will need to read the targets in from the database so we have
        # something to build the tiles from
        logging.debug('Reading in list of guide targets from DB')
        return_targets = True
        guide_targets = rG.execute(cursor)

    if standard_targets is None:
        # Will need to read the targets in from the database so we have
        # something to build the tiles from
        logging.debug('Reading in list of standard targets from DB')
        return_targets = True
        standard_targets = rSt.execute(cursor)

    all_targets = candidate_targets + guide_targets + standard_targets
    logging.debug('Sorting all-targets list...')
    all_targets.sort(key=lambda x: x.idn)
    logging.debug('Generating list of sorted target IDs')
    all_targets_ids = [t.idn for t in all_targets]

    # Get the fibre assignments
    fibreassigns = extract_from_joined(cursor,
                                       ['field', 'tile', 'target_field'],
                                       conditions=None,
                                       columns=['tile_pk',
                                                'field_id',
                                                'ra',
                                                'dec',
                                                'bug_id',
                                                'target_id'])

    tile_list = []

    # Re-construct the tiles from the list we got from the database
    # Construct a list of tile_pks present

    # TODO: This block is *very* slow - look for improvements
    logging.debug('Getting list of unique tile PKs')
    pks = list(set([row['tile_pk'] for row in fibreassigns]))
    logging.debug('Assigning targets to tiles...')
    for pk in pks:
        # Assign TaipanTarget objects from candidate_targets to the tile
        # Select out the relevant rows of fibreassigns
        logging.debug('Selecting entries for tile PK %d' % pk)
        bugs = [row for row in fibreassigns if row['tile_pk'] == pk]

        # Create the tile
        new_tile = TaipanTile(bugs[0]['ra'], bugs[0]['dec'],
                              field_id=bugs[0]['field_id'],
                              pk=bugs[0]['tile_pk'])

        # Assign the targets
        logging.debug('Assigning targets')
        for bugassign in bugs:
            if bugassign['target_id'] != SKY_TARGET_ID:
                new_tile.set_fibre(bugassign['bug_id'],
                                   all_targets[index(all_targets_ids,
                                       bugassign['target_id']
                                   )])
            else:
                new_tile.set_fibre(bugassign['bug_id'], 'sky')

        # Append the new_tile to the return list
        tile_list.append(new_tile)

    if return_targets:
        return tile_list, candidate_targets, guide_targets, standard_targets

    return tile_list
