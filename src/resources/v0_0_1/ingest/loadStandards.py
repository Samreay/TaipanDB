import logging
from astropy.table import Table
from ....scripts.create import insert_many_rows
from taipan.core import polar2cart


def execute(cursor, standards_file=None):
    logging.info("Loading Standards")

    if not standards_file:
        logging.info("No file passed - aborting loading standards")
        return

    # Get guides
    standards_table = Table.read(standards_file)

    values_table = [[row['objID'] + int(1e9)*row['reference'],
                     float(row['ra_SCOS']),
                     float(row['dec_SCOS']),
                     False, True, False] +
                    list(polar2cart((row['ra_SCOS'], row['dec_SCOS'])))
                    for row in standards_table]
    columns = ["TARGET_ID", "RA", "DEC", "IS_SCIENCE", "IS_STANDARD",
               "IS_GUIDE", "UX", "UY", "UZ"]

    # Insert into database
    if cursor is not None:
        insert_many_rows(cursor, "target", values_table, columns=columns)
        logging.info("Loaded Standards")
    else:
        logging.info('No database - returning values to console')
        return values_table

    return
