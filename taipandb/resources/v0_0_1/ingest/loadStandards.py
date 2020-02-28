import logging
from astropy.table import Table
from ....scripts.create import insert_many_rows, insert_row
from taipan.core import polar2cart


def execute(cursor, standards_file=None, mark_active=True,
            ra_ranges=[], dec_ranges=[]):
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

    if standards_file.split('/')[-1] == 'Fstar_Panstarrs.fits':
        values_table = [[int(row.index) + 1e11,
                         # + int(1e9)*row['reference'],
                         float(row['RASTACK']),  # RASTACK
                         float(row['DECSTACK']),  # DECSTACK
                         0.0, 0.0,
                         False, True, False, False,
                         True, None] +
                        list(polar2cart((row['RASTACK'], row['DECSTACK'])))
                        for row in standards_table]
    elif standards_file.split('/')[-1] == 'Fstar_skymapperdr1.fits':
        values_table = [[int(row.index) + 5e11,
                         # + int(1e9)*row['reference'],
                         float(row['RAJ2000']),  # RAJ2000
                         float(row['DEJ2000']),  # DEJ2000
                         0.0, 0.0,
                         False, True, False, False,
                         True, None] +
                        list(polar2cart((row['RAJ2000'], row['DEJ2000'])))
                        for row in standards_table]
    else:
        values_table = [[int(row['objID']),
                         # + int(1e9)*row['reference'],
                         float(row['ra_SCOS']),   # ra_SCOS
                         float(row['dec_SCOS']),  # dec_SCOS
                         0.0, 0.0,
                         False, True, False, False,
                         True, None] +
                        list(polar2cart((row['ra_SCOS'], row['dec_SCOS'])))
                        for row in standards_table]

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
        logging.info("Loaded Standards")
    else:
        logging.info('No database - returning values to console')
        return values_table

    # Insert the special target to be used as the sky target
    # Now moved to loadSkies
    # if cursor is not None:
    #     logging.info('Inserting special sky target into DB')
    #     insert_row(cursor, "target", [SKY_TARGET_ID, 0.0, 0.0,  # ID, RA, DEC
    #                                   0.0, 0.0, 0.0,            # ux, uy, uz
    #                                   False, False, False, False])     # sci, gui, stan, sky

    return
