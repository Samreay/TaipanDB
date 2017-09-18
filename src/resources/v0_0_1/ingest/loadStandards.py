import logging
from astropy.table import Table
from ....scripts.create import insert_many_rows, insert_row
from taipan.core import polar2cart
from ...v0_0_1 import SKY_TARGET_ID


def execute(cursor, standards_file=None, mark_active=True):
    """
    Load standard targets from file into the database.

    Parameters
    ----------
    cursor: :any:`psycopg2.connection.cursor`
        psycopg2 cursor for interacting with the database.
    standards_file: :obj:`str`
        File to load the standard targets from. Defaults to None, at which point
        the function will abort.
    mark_active: :obj:`bool`
        Denotes whether these targets should be marked as active in the
        database. Defaults to True.

    Returns
    -------
    :obj:`None`
        Standard targets are loaded into the database.
    """
    logging.info("Loading Standards")

    if not standards_file:
        logging.info("No file passed - aborting loading standards")
        return

    # Get guides
    standards_table = Table.read(standards_file)
    # logging.debug(standards_table)

    values_table = [[int(row['objID']),
                     # + int(1e9)*row['reference'],
                     float(row['ra_SCOS']),
                     float(row['dec_SCOS']),
                     False, True, False,
                     True] +
                    list(polar2cart((row['ra_SCOS'], row['dec_SCOS'])))
                    for row in standards_table]
    columns = ["TARGET_ID", "RA", "DEC", "IS_SCIENCE", "IS_STANDARD",
               "IS_GUIDE", "IS_ACTIVE", "UX", "UY", "UZ"]

    # Insert into database
    if cursor is not None:
        insert_many_rows(cursor, "target", values_table, columns=columns)
        logging.info("Loaded Standards")
    else:
        logging.info('No database - returning values to console')
        return values_table

    # Insert the special target to be used as the sky target
    if cursor is not None:
        logging.info('Inserting special sky target into DB')
        insert_row(cursor, "target", [SKY_TARGET_ID, 0.0, 0.0,  # ID, RA, DEC
                                      0.0, 0.0, 0.0,            # ux, uy, uz
                                      False, False, False])     # sci, gui, stan

    return
