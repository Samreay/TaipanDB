import logging
from astropy.table import Table
import sys
import os
sys.path.append(os.path.realpath(os.path.dirname(os.path.abspath(__file__))
                                 + "/../../.."))
from scripts.create import insert_many_rows

from taipan.core import polar2cart


def execute(cursor, science_file=None):
    logging.info("Loading Science")

    if not science_file:
        logging.info("No file passed - aborting loading science")
        return

    # Get science
    science_table = Table.read(science_file)

    # FOR TEST/FUDGE USE ONLY
    # Step through the table, and where we find a duplicate ID, alter it
    # by adding id*1e9

    # Do some stuff to convert science_table into values_table
    # (This is dependent on the structure of science_file)
    logging.debug('Creating tables for database load')
    values_table1 = [[row['uniqid'] + int(1e9)*row['reference'],
                      float(row['ra']), float(row['dec']),
                      True, False, False] 
                      + list(polar2cart((row['ra'], row['dec'])))
                     for row in science_table]
    columns1 = ["TARGET_ID", "RA", "DEC", "IS_SCIENCE", "IS_STANDARD",
                "IS_GUIDE", "UX", "UY", "UZ"]
    values_table2 = [[row['uniqid'] + int(1e9)*row['reference'],
                      row['priority'],
                      bool(row['is_H0']), bool(row['is_vpec']),
                      bool(row['is_lowz'])]
                     for row in science_table]
    columns2 = ["TARGET_ID", "PRIORITY", "IS_H0_TARGET", "IS_VPEC_TARGET",
                "IS_LOWZ_TARGET"]

    logging.debug('Loading to cursor')
    # Insert into database
    if cursor is not None:
        insert_many_rows(cursor, "target", values_table1, columns=columns1)
        insert_many_rows(cursor, "science_target", values_table2,
                         columns=columns2)
        logging.info("Loaded Science")
    else:
        logging.info("No database - however, dry-run of loading successful")
