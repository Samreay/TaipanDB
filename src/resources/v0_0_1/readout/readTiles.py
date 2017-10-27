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
    a : :obj:`list`
        Input list
    x
        Value to search for

    Returns
    -------
    i : :obj:`int`
        The index of the left-most value in a exactly equal to x
    """
    i = bisect_left(a, x)
    if i != len(a) and a[i] == x:
        return i
    raise ValueError('Value x is not in list a!')


def execute(cursor, tile_pks=None, field_ids=None,
            candidate_targets=None, guide_targets=None,
            standard_targets=None):
    """
    Return a list of tiles from the database.

    Parameters
    ----------
    cursor: :obj:`psycopg2.connection.cursor`
        psycopg2 cursor for interacting with the database
    tile_pks: :obj:`list` of :obj:`int`, optional
        List of tile PKs to return information for. Defaults to None,
        at which point all tiles in the database will be returned.
    candidate_targets, guide_targets, standard_targets: :obj:`list` of :obj:`taipan.core.TaipanTarget`
        Optional; lists of TaipanTargets corresponding to science targets,
        guide targets and standard targets, which are required to be
        assigned to the TaipanTiles being returned. Defaults to None, at
        which point the lists will be read in from the database.

    Returns
    -------
    tile_list, candidate_targets, guide_targets, standard_targets: :obj:`list` of :obj:`taipan.core.TaipanTarget`
        A list of TaipanTile objects corresponding to the tiles in the
        database. If target lists are not passed into the function, the
        function will return the target lists it generates (so that repeat
        calls to this function do not need to repeatedly read the lists).
    """

    return_targets = False

    if cursor is None and (candidate_targets is None or
                           standard_targets is None):
        raise RuntimeError('If using readTiles without a cursor, '
                           'you must provide all three target lists')

    if tile_pks and field_ids:
        raise ValueError('Can only specify one of tile_pks and field_ids')

    logging.info('Reading tiles from database')

    conditions = []

    if tile_pks is not None:
        tile_pks = list(tile_pks)
        conditions += [
            ('tile_pk', 'IN', tile_pks),
        ]
    if field_ids is not None:
        field_ids = list(field_ids)
        conditions += [
            ('field_id', 'IN', field_ids)
        ]

    # Get the fibre assignments
    fibreassigns = extract_from_joined(cursor,
                                       ['field', 'tile', 'target_field'],
                                       conditions=conditions,
                                       columns=['tile_pk',
                                                'field_id',
                                                'ra',
                                                'dec',
                                                'bug_id',
                                                'target_id'])

    if candidate_targets is None:
        # Will need to read the targets in from the database so we have
        # something to build the tiles from
        logging.debug('Reading in list of science targets from DB')
        return_targets = True
        candidate_targets = rS.execute(cursor,
                                       target_ids=
                                       list(fibreassigns['target_id']))

    if guide_targets is None:
        # Will need to read the targets in from the database so we have
        # something to build the tiles from
        logging.debug('Reading in list of guide targets from DB')
        return_targets = True
        guide_targets = rG.execute(cursor, field_list=fibreassigns['field_id'])

    if standard_targets is None:
        # Will need to read the targets in from the database so we have
        # something to build the tiles from
        logging.debug('Reading in list of standard targets from DB')
        return_targets = True
        standard_targets = rSt.execute(cursor,
                                       field_list=fibreassigns['field_id'])

    all_targets = candidate_targets + guide_targets + standard_targets
    logging.debug('Sorting all-targets list...')
    all_targets.sort(key=lambda x: x.idn)
    logging.debug('Generating list of sorted target IDs')
    all_targets_ids = [t.idn for t in all_targets]

    tile_list = []

    # Re-construct the tiles from the list we got from the database
    # Construct a list of tile_pks present
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
