import logging
from scripts.create import insert_into


def execute(cursor, science_file=None):
    logging.info("Loading Science")

    if not guides_file:
        logging.info("No file passed - aborting loading science")
        return

    # Get science
    science_table = Table.read(science_file)

    # Do some stuff to convert science_table into values_table
    # (This is dependent on the structure of science_file)
    values_table1 = list(science_table)
    columns1 = ["TARGET_ID", "RA", "DEC", "IS_SCIENCE", "IS_STANDARD",
                "IS_GUIDE"]
    values_table2 = list(science_table)
    columns2 = ["Target", "is_H0", "is_vpec", "is_lowz"]

    # Insert into database
    if cursor is not None:
        insert_into(cursor, "target", values_table1, columns=columns1)
        insert_into(cursor, "science_target", values_table2, columns=columns2)

    logging.info("Loaded Science")
