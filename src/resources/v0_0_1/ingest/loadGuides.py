from ....scripts.create import insert_many_rows
from taipan.core import polar2cart
import logging
from astropy.table import Table


def execute(cursor, guides_file=None, mark_active=True):
    """
    Insert guide targets from file into the database.

    Parameters
    ----------
    cursor: :any:`psycopg2.connection.cursor`
        psycopg2 cursor for interacting with the database.
    guides_file: :obj:`str`
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
    logging.info("Loading Guides")

    if not guides_file:
        logging.info("No file passed - aborting loading guides")
        return

    # Get guides
    guides_table = Table.read(guides_file)

    values_table = [[
                     # int(''.join(row['ucacid'].split('-')[1:])) + int(4e9),
                     int(row['ucacid']),
                     float(row['raj2000']),
                     float(row['dej2000']),
                     False, False, True,
                    mark_active]
                    + list(polar2cart((row['raj2000'], row['dej2000'])))
                    for row in guides_table]
    columns = ["TARGET_ID", "RA", "DEC", "IS_SCIENCE", "IS_STANDARD",
               "IS_GUIDE", "IS_ACTIVE", "UX", "UY", "UZ"]

    # Insert into database
    if cursor is not None:
        insert_many_rows(cursor, "target", values_table, columns=columns)
        logging.info("Loaded Guides")
    else:
        logging.info('No database - returning values to console')
        return values_table

    return
