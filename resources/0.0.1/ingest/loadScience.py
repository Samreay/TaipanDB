import logging
from astropy.table import Table
from scripts.create import insert_into


def execute(cursor, science_file=None):
    logging.info("Loading Science")

    if not science_file:
        logging.info("No file passed - aborting loading science")
        return

    # Get science
    science_table = Table.read(science_file)

    # Do some stuff to convert science_table into values_table
    # (This is dependent on the structure of science_file)
    values_table1 = [[row['uniqid'], row['ra'], row['dec'],
                      True, False, False]
                     for row in science_table]
    columns1 = ["TARGET_ID", "RA", "DEC", "IS_SCIENCE", "IS_STANDARD",
                "IS_GUIDE"]
    values_table2 = [[row['uniqid'],
                      row['is_H0'], row['is_vpec'], row['is_lowz']]
                     for row in science_table]
    columns2 = ["TARGET_ID", "IS_H0", "IS_VPEC", "IS_LOWZ"]

    # Insert into database
    if cursor is not None:
        insert_into(cursor, "target", values_table1, columns=columns1)
        insert_into(cursor, "science_target", values_table2, columns=columns2)
        logging.info("Loaded Science")
    else:
        logging.info("No database - however, dry-run of loading successful")
