# Get a list of fields (centroids) that may be affected my changes to tiles
# or fields passed as inputs

import logging
from ....scripts.extract import extract_from, extract_from_joined
from readCentroids import execute as rCexec
from taipan.core import TaipanTile

import numpy as np

def execute(cursor, field_list=None, tile_list=None):
    """
    Calculate which fields will be affected by changes to the fields/tiles
    passed as inputs, and return a list of them.

    The user should pass ONLY field_list OR tile_list. Passing neither will
    raise a ValueError; passing both will cause field_list to take priority.

    Parameters
    ----------
    cursor:
        psycopg2 cursor for interacting with the database
    field_list:
        Optional, list of field_id values (ints) corresponding to the fields
        that may change. Note that this will override tile_list if passed.
    tile_list:
        Optional, list of tile_pk values (ints) corresponding to the tiles
        that may have changed.

    Returns
    -------
    field_ids:
        A numpy structured array of info on fields that may be affected
        by changes to the fields/tiles in the input lists (i.e. the fields
        that overlap the fields/tiles passed in).

    """

    # Input checking
    if field_list is None and tile_list is None:
        raise ValueError('Must pass one of field_list or tile_list')

    if field_list is not None and tile_list is not None:
        # Force field_list to take priority
        tile_list = None

    # By this point, exactly one of field_list/tile_list is None and one isn't
    # Make sure we actually have lists, or something that can be turned into
    # lists
    if tile_list is None:
        tile_list = list(field_list)
    else:
        tile_list = list(tile_list)

    # Pull in all of the existing fields
    fields_tileobjs = rCexec(cursor)

    # Pull in the fields or tiles which have been requested by the user
    if tile_list is not None:
        req_tileobjs = extract_from_joined(cursor, ['tile','field'],
                                           conditions=[
                                               ('tile_pk', 'IN',
                                                tuple(tile_list)),
                                           ],
                                           columns=['tile_pk', 'field_id', 'ra',
                                                    'dec', 'ux', 'uy', 'uz'])
        logging.debug(req_tileobjs)
    else:
        req_tileobjs = [f for f in fields_tileobjs if
                        f.field_id in field_list]

    return