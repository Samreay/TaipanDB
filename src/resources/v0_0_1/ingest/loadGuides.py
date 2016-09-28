from ....scripts.create import insert_many_rows
from taipan.core import polar2cart
import logging
from astropy.table import Table


def execute(cursor, guides_file=None):
    """
    Insert guide targets from file into the database.

    Parameters
    ----------
    cursor:
        psycopg2 cursor for interacting with the database.
    guides_file:
        The file from which to load guides. Defaults to None, at which point
        the function will abort.

    Returns
    -------
    Nil. Guide targets are inserted into the database.
    """
    logging.info("Loading Guides")

    if not guides_file:
        logging.info("No file passed - aborting loading guides")
        return

    # Get guides
    guides_table = Table.read(guides_file)

    values_table = [[int(''.join(row['ucacid'].split('-')[1:])) + int(4e9),
                     float(row['ra_SCOS']),
                     float(row['dec_SCOS']),
                     False, False, True]
                    + list(polar2cart((row['ra_SCOS'], row['dec_SCOS'])))
                    for row in guides_table]
    columns = ["TARGET_ID", "RA", "DEC", "IS_SCIENCE", "IS_STANDARD",
               "IS_GUIDE", "UX", "UY", "UZ"]

    # Insert into database
    if cursor is not None:
        insert_many_rows(cursor, "target", values_table, columns=columns)
        logging.info("Loaded Guides")
    else:
        logging.info('No database - returning values to console')
        return values_table

    return
