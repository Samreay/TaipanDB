# Configure the VM database to version 0.0.1
# This is a convenience function for calling the relevant parts of the
# resources structure
# This script *must* be run within the /data/code/TaipanDB/resources/0.0.1
# directory (i.e. that must be the pwd when invoked)

from scripts.create import destroy_tables, create_tables
from scripts.connection import get_connection
import logging
from ingest import *
from manipulate import *

tables_dir = 'tables/'
data_dir = '/data/resources/0.0.1/'


def execute(cursor):
    """
    Execute the configuration process.

    Parameters
    ----------
    cursor:
        psycopg2 cursor for interacting with the database.

    Returns
    -------
    Nil. VM databased configured & loaded in place.
    """

    # Create & destroy the tables
    # Note this requires user input
    logging.info('INIT - destroying & recreating tables')
    destroy_tables(cursor)
    create_tables(cursor, tables_dir)

    # Populate the tables
    logging.info('INIT - loading centroids')
    loadCentroids.execute(cursor, fields_file=datadir+'pointing_centers.radec')
    cursor.connection.commit()

    logging.info('INIT - loading guides')
    loadGuides.execute(cursor, guides_file=datadir+'SCOSxAllWISE.photometry.'
                                                   'forTAIPAN.reduced.guides_'
                                                   'nodups.fits')
    cursor.connection.commit()

    logging.info('INIT - loading standards')
    loadStandards.execute(cursor, standards_file='SCOSxAllWISE.photometry.'
                                                 'forTAIPAN.reduced.standards_'
                                                 'nodups.fits')
    cursor.connection.commit()

    logging.info('INIT - loading science targets')
    loadScience.execute(cursor, science_file='priority_science.v0.101_'
                                             '20160331.fits')
    cursor.connection.commit()

    # Update the difficulties for the science targets
    logging.info('INIT - computing & entering sci target difficulties')
    makeScienceDiff.execute(cursor)
    cursor.connection.commit()

    logging.info('INIT - init complete')


if __name__ == '__main__':
    # Get a cursor
    conn = get_connection()
    cursor = conn.cursor()

    # Execute
    execute(cursor)