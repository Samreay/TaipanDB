import logging
from ....scripts.extract import extract_from, extract_from_joined
from taipan.core import TaipanTile, BUGPOS_OFFSET

import readScience as rS
import readGuides as rG
import readStandards as rSt


def execute(cursor, candidate_targets=None, guide_targets=None,
            standard_targets=None):

    logging.info('Reading tiles from database')

    if candidate_targets is None:
        # Will need to read the targets in from the database so we have
        # something to build the tiles from
        logging.debug('Reading in list of science targets from DB')
        return_candidate_targets = True
        candidate_targets = rS.execute(cursor)

    if guide_targets is None:
        # Will need to read the targets in from the database so we have
        # something to build the tiles from
        logging.debug('Reading in list of guide targets from DB')
        return_candidate_targets = True
        guide_targets = rG.execute(cursor)

    if standard_targets is None:
        # Will need to read the targets in from the database so we have
        # something to build the tiles from
        logging.debug('Reading in list of standard targets from DB')
        return_candidate_targets = True
        standard_targets = rS.execute(cursor)

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
        # Get the field ID
        # TODO: Determine a more efficient way to do this
        fid = [row['field_id'] for row in fibreassigns if
               row['tile_pk'] == pk][0]
        # Get the field RA and DEC from fieldposns
        ra, dec = [(row['ra'], row['dec']) for row in fieldposns if
                   row['field_id'] == fid][0]
        # Create the tile
        new_tile = TaipanTile(ra, dec, field_id=fid, pk=pk)
        # Assign TaipanTarget objects from candidate_targets to the tile
        bugs = [row for row in fibreassigns if row['tile_pk'] == pk]
        for bugassign in bugs:
            if bugassign['target_id'] is not None:
                target_gen = (i for i,v in
                              enumerate(candidate_targets +
                                        guide_targets +
                                        standard_targets) if
                              v.idn == bugassign['target_id'])
                new_tile.set_fibre(bugassign['bug_id'],
                                   candidate_targets[next(target_gen)])
        # Any bugs not listed in the database are 'sky' - set
        # these as such
        for bug in [bug for bug in BUGPOS_OFFSET if bug not in bugs['bug_id']]:
            new_tile.set_fibre(bug, 'sky')

        # Append the new_tile to the return list
        tile_list.append(new_tile)


    if return_candidate_targets:
        return tile_list, candidate_targets, guide_targets, standard_targets

    return tile_list
