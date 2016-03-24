import logging
from scripts.create import insert_into


def execute(cursor, standards_file=None):
    logging.info("Loading Standards")

    if not standards_file:
        logging.info("No file passed - aborting loading standards")
        return

    # Get guides
    standards_table = Table.read(standards_file)

    # Do some stuff to convert guides_table into values_table
    # (This is dependent on the structure of guides_file)
    values_table = list(standards_table)
    columns = ["TARGET_ID", "RA", "DEC", "IS_SCIENCE", "IS_STANDARD",
               "IS_GUIDE"]

    # Insert into database
    if cursor is not None:
        insert_into(cursor, "target", values_table, columns=columns)

    logging.info("Loaded Standards")
