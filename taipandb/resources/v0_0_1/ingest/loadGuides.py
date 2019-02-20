from ....scripts.create import insert_many_rows
from taipan.core import polar2cart
import logging
from astropy.table import Table


def execute(cursor, guides_file=None, mark_active=True,
            ra_col='RA', dec_col='DEC', mag_col='VMAG',
            pm_ra_col='PMRAC', pm_dec_col='PMDEC',
            ra_ranges=[], dec_ranges=[]):
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
    ra_col, dec_col, mag_col: str
        The input catalogue columns names for target RA, Dec and V-band
        magnitude, respectively.

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
                     int(row.index + 1e9),
                     float(row[ra_col]),
                     float(row[dec_col]),
                     float(row[pm_ra_col]),
                     float(row[pm_dec_col]),
                     False, False, True, False,
                     float(row[mag_col]),
                    mark_active]
                    + list(polar2cart((row[ra_col], row[dec_col])))
                    for row in guides_table]
    if ra_ranges:
        values_table = [row for row in values_table if
                        any([r[0] <= row[1] <= r[1] for r in ra_ranges])]
    if dec_ranges:
        values_table = [row for row in values_table if
                        any([r[0] <= row[2] <= r[1] for r in dec_ranges])]

    columns = ["TARGET_ID",
               "RA", "DEC",
               "PM_RA", "PM_DEC",
               "IS_SCIENCE", "IS_STANDARD", "IS_GUIDE", "IS_SKY",
               "MAG",
               "IS_ACTIVE", "UX", "UY", "UZ"]

    # Insert into database
    if cursor is not None:
        insert_many_rows(cursor, "target", values_table, columns=columns)
        logging.info("Loaded Guides")
    else:
        logging.info('No database - returning values to console')
        return values_table

    return
