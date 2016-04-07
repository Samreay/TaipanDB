import logging
from astropy.table import Table
from scripts.create import insert_many_rows

from taipan.core import polar2cart

def execute(cursor, guides_file=None):
    logging.info("Loading Guides")

    if not guides_file:
        logging.info("No file passed - aborting loading guides")
        return

    # Get guides
    guides_table = Table.read(guides_file)

    values_table = [[row['objID'], row['ra_SCOS'], row['dec_SCOS'],
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
