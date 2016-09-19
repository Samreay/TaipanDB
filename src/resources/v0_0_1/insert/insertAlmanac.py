# Insert calculated almanacs into the database

import logging
from taipan.core import TaipanTile
from ....scripts.create import insert_many_rows
from ....scripts.extract import extract_from, extract_from_joined

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


def execute(cursor, field_id, almanac_data):
    """
    Insert the given tiles into the database.

    Parameters
    ----------
    cursor:
        psycopg2 cursor for interacting with the database
    field_id:
        The field_id for the almanac to be written. This field must already
        exist in the field table.
    almanac_data:
        The almanac data to write to disk. This should be a list of 3-tuples,
        where each tuple takes the form
        (datetime, airmass, sun_alt, is_dark)
        with data types
        (datetime.datetime, float, float, boolean)
        respectively.


    Returns
    -------
    Nil. Almanacs are written directly into the database.
    """
    logging.info('Inserting almanacs into database...')


    return
