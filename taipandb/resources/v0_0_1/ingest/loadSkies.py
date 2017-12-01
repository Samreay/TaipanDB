from ....scripts.create import insert_many_rows
from taipan.core import polar2cart
from ....scripts.create import insert_many_rows, insert_row
import logging
from astropy.table import Table

from ...v0_0_1 import SKY_TARGET_ID


def execute(cursor, skies_file=None, mark_active=True):
    """
    Insert sky targets from file into the database.

    Parameters
    ----------
    cursor: :any:`psycopg2.connection.cursor`
        psycopg2 cursor for interacting with the database.
    skies_file: :obj:`str`
        The file from which to load guides. Defaults to None, at which point
        the function will abort.
    mark_active: :obj:`bool`
        Denotes whether these targets should be marked as active in the
        database. Defaults to True.

    Returns
    -------
    :obj:`None`
        Guide targets are inserted into the database.
    """
    logging.info("Loading Skies")

    if not skies_file:
        logging.info("No file passed - aborting loading guides")
        return

    # Get guides
    guides_table = Table.read(skies_file)

    values_table = [[
                     # int(''.join(row['ucacid'].split('-')[1:])) + int(4e9),
                     int(row['pkeyid']) + int(1e12),
                     float(row['ra']),
                     float(row['dec']),
                     False, False, False, True,
                    mark_active]
                    + list(polar2cart((row['raj2000'], row['dej2000'])))
                    for row in guides_table]
    columns = ["TARGET_ID", "RA", "DEC", "IS_SCIENCE", "IS_STANDARD",
               "IS_GUIDE", "IS_SKY", "IS_ACTIVE", "UX", "UY", "UZ"]

    # Insert into database
    if cursor is not None:
        insert_many_rows(cursor, "target", values_table, columns=columns)
        logging.info("Loaded Skies")

        # Insert the special target to be used as the sky target
        if cursor is not None:
            logging.info('Inserting special sky target into DB')
            insert_row(cursor, "target",
                       [SKY_TARGET_ID, 0.0, 0.0,  # ID, RA, DEC
                        0.0, 0.0, 0.0,  # ux, uy, uz
                        False, False, False, False])  # sci, gui, stan, sky
    else:
        logging.info('No database - returning values to console')
        return values_table

    return
