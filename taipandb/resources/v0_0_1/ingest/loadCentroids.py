from __future__ import absolute_import
import logging
import pandas as pd
import os
import sys

from ....scripts.create import insert_many_rows
from ....scripts.extract import select_max_from_joined
from taipan.core import polar2cart
from taipan.scheduling import POLE_EXCLUSION_DISTANCE


def execute(cursor, fields_file=None, mark_active=True,
            ra_ranges=[], dec_ranges=[],
            pole_exclusion_radius=POLE_EXCLUSION_DISTANCE):
    """
    Load field pointings from file to database

    Parameters
    ----------
    cursor: :any:`psycopg2.connection.cursor`
        psycopg2 cursor for interacting with the database
    fields_file: :obj:`str`
        Path and name of the file holding the field information. Defaults to
        None, at which point the function will abort.
    mark_active: :obj:`bool`
        Denotes whether these targets should be marked as active in the
        database. Defaults to True.
    ra_ranges, dec_ranges: List of two-tuples of ints
        Specifies RA and/or Dec ranges that centroids should be restricted to
        lie within. Multiple ranges may be specified for both RA and Dec.
    pole_exclusion_radius: float, default :any:`taipan.scheduling.POLE_EXCLUSION_RADIUS`
        Do not ingest any centroids within pole_exclusion_radius of a
        celestial pole. Set to zero to have no effect.

    Returns
    -------
    :obj:`None`
        Fields are loaded into the database.
    """

    logging.info("Loading Centroids")

    if not fields_file:
        logging.info("No tiling file passed - aborting loading centroids")
        return

    # Get the maximal field_id in the database
    max_field_id = select_max_from_joined(cursor, ['field'], 'field_id')
    if max_field_id is None:
        max_field_id = 0

    # Get centroids
    with open(fields_file, 'r') as fileobj:
        datatable = pd.read_csv(fileobj, delim_whitespace=True)
    values = [[int(index + max_field_id + 1),
               float(row['ra']), float(row['dec']),
               mark_active]
              + list(polar2cart((row['ra'], row['dec'])))
              for index, row in datatable.iterrows()]

    # Do pole exclusion
    values = [row for row in values if
              90. - abs(row[2]) < pole_exclusion_radius]

    if ra_ranges:
        values = [row for row in values if
                  any([r[0] <= row[1] <= r[1] for r in ra_ranges])]
    if dec_ranges:
        values = [row for row in values if
                  any([r[0] <= row[2] <= r[1] for r in dec_ranges])]



    columns = ["FIELD_ID", "RA", "DEC", "IS_ACTIVE", "UX", "UY", "UZ"]

    # Insert into database
    if cursor is not None:
        insert_many_rows(cursor, "field", values, columns=columns)
        logging.info('Loaded Centroids')
    else:
        logging.info('No DB to write to - returning values')
        return values

    return
