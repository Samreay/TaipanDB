import logging
from astropy.table import Table
from scripts.create import insert_into


def execute(cursor, guides_file=None):
    logging.info("Loading Guides")

    if not guides_file:
        logging.info("No file passed - aborting loading guides")
        return

    # Get guides
    guides_table = Table.read(guides_file)

    # Do some stuff to convert guides_table into values_table
    # (This is dependent on the structure of guides_file)
    values_table = list(guides_table)
    columns = ["TARGET_ID", "RA", "DEC", "IS_SCIENCE", "IS_STANDARD",
               "IS_GUIDE"]

    # Insert into database
    if cursor is not None:
        insert_into(cursor, "target", values_table, columns=columns)

    logging.info("Loaded Guides")
