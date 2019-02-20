from ....scripts.create import insert_many_rows
from taipan.core import polar2cart
from ....scripts.create import insert_many_rows, insert_row
import logging
from astropy.table import Table

from taipandb.resources.v0_0_1 import SKY_TARGET_ID


def execute(cursor, skies_file=None, mark_active=True,
            ra_col='ra', dec_col='dec', ra_ranges=[], dec_ranges=[]):
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
    ra_col, dec_col, : str
        The input catalogue columns names for target RA and Dec, respectively.

    Returns
    -------
    :obj:`None`
        Guide targets are inserted into the database.
    """
    logging.info("Loading Skies")

    # if not skies_file:
    #     logging.info("No file passed - aborting loading guides")
    #     return

    # Get guides
    guides_table = Table.read(skies_file)

    values_table = [[
                        # int(''.join(row['ucacid'].split('-')[1:])) + int(4e9),
                        int(row['pkey_id']) + int(1e12),
                        float(row[ra_col]),
                        float(row[dec_col]),
                        0.0, 0.0,
                        False, False, False, True,
                        mark_active, None,]
                    + list(polar2cart((row[ra_col], row[dec_col])))
                    for row in guides_table]
    if ra_ranges:
        values_table = [row for row in values_table if
                        any([r[0] <= row[1] <= r[1] for r in ra_ranges])]
    if dec_ranges:
        values_table = [row for row in values_table if
                        any([r[0] <= row[2] <= r[1] for r in dec_ranges])]

    columns = ["TARGET_ID", "RA", "DEC",
               "PM_RA", "PM_DEC",
               "IS_SCIENCE", "IS_STANDARD",
               "IS_GUIDE", "IS_SKY", "IS_ACTIVE", "MAG", "UX", "UY", "UZ"]

    # Insert into database
    if cursor is not None:
        insert_many_rows(cursor, "target", values_table, columns=columns)
        logging.info("Loaded Skies")

        # Insert the special target to be used as the sky target
        if cursor is not None:
            logging.info('Inserting special sky target into DB')
            insert_row(cursor, "target",
                       [SKY_TARGET_ID, 0.0, 0.0,  # ID, RA, DEC
                        0.0, 0.0,  # PM_RA, PM_DEC
                        0.0, 0.0, 0.0,  # ux, uy, uz
                        None,  # mag
                        False, False, False, False])  # sci, gui, stan, sky
    else:
        logging.info('No database - returning values to console')
        return values_table

    return
