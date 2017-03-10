from __future__ import absolute_import
import logging
import pandas as pd
import os
import sys

from ....scripts.create import insert_many_rows
from taipan.core import polar2cart


def execute(cursor, fields_file=None, mark_active=True):
    """
    Load field pointings from file to database

    Inputs
    ------
    cursor:
        psycopg2 cursor for interacting with the database
    fields_file:
        Path and name of the file holding the field information. Defaults to
        None, at which point the function will abort.

    Returns
    -------
    Nil. Fields are loaded into the database.
    """

    logging.info("Loading Centroids")

    if not fields_file:
        logging.info("No tiling file passed - aborting loading centroids")
        return

    # Get centroids
    with open(fields_file, 'r') as fileobj:
        datatable = pd.read_csv(fileobj, delim_whitespace=True)
    values = [[index, row['ra'], row['dec'], mark_active]
              + list(polar2cart((row['ra'], row['dec'])))
              for index, row in datatable.iterrows()]

    columns = ["FIELD_ID", "RA", "DEC", "IS_ACTIVE", "UX", "UY", "UZ"]

    # Insert into database
    if cursor is not None:
        insert_many_rows(cursor, "field", values, columns=columns)
        logging.info('Loaded Centroids')
    else:
        logging.info('No DB to write to - returning values')
        return values

    return
