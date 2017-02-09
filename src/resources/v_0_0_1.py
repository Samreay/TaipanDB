from ..scripts import create
from .v0_0_1.ingest import loadGuides, loadScience, loadCentroids, loadStandards
from .v0_0_1.manipulate import makeScienceDiff, makeTargetPosn

from src.resources.v0_0_1.insert.insertAlmanac import execute as iAexec

from ..resources.v0_0_1.readout.readCentroids import execute as rCexec

from taipan.scheduling import Almanac, DarkAlmanac

from src.scripts.connection import get_connection

import os
import datetime
import logging


def update(cursor):
    resource_dir = os.path.dirname(__file__) + os.sep + "v0_0_1" + os.sep
    data_dir = "/data/resources/0.0.1/"
    table_dir = resource_dir + os.sep + "tables"

    create.create_tables(cursor, table_dir)

    fields_file = data_dir + "pointing_centers.radec"
    loadCentroids.execute(cursor, fields_file=fields_file)

    # guides_file = data_dir + "SCOSxAllWISE.photometry.forTAIPAN." \
                             # "reduced.guides_nodups.fits"
    # guides_file = data_dir + 'guides_UCAC4_btrim.fits'
    guides_file = data_dir + 'random_mock_guides_160930.fits'
    loadGuides.execute(cursor, guides_file=guides_file)

    # standards_file = data_dir + 'SCOSxAllWISE.photometry.forTAIPAN.' \
    #                             'reduced.standards_nodups.fits'
    standards_file = data_dir + 'random_mock_standards_160928.fits'
    loadStandards.execute(cursor, standards_file=standards_file)

    # science_file = data_dir + 'priority_science.v0.101_20160331.fits'
    science_file = data_dir + 'Taipan_mock_inputcat_v1.1_170208.fits'
    loadScience.execute(cursor, science_file=science_file)

    # Commit here in case something further along fails
    cursor.connection.commit()

    makeScienceDiff.execute(cursor)

    makeTargetPosn.execute(cursor)

    # Commit again
    cursor.connection.commit()

    # Instantiate the Almanacs
    sim_start = datetime.date(2017, 4, 1)
    sim_end = datetime.date(2027, 4, 1)
    global_start = datetime.datetime.now()

    fields = rCexec(cursor)

    logging.info('Generating dark almanac...')
    dark_alm = DarkAlmanac(sim_start, end_date=sim_end)
    logging.info('Done!')

    i = 1
    for field in fields:
        almanac = Almanac(field.ra, field.dec, sim_start, end_date=sim_end,
                          minimum_airmass=2.0, populate=True, resolution=15.)
        logging.info('Computed almanac %5d / %5d' % (i, len(fields),))
        iAexec(cursor, field.field_id, almanac, dark_almanac=dark_alm)
        i += 1
        # Commit after every Almanac due to the expense of computing
        cursor.connection.commit()
        logging.info('Inserted almanac %5d / %5d' % (i, len(fields),))

    # Create a date-only index on the observability table to improve performance
    create.create_index(cursor, 'observability', ['date'], ordering='ASC')

    return


if __name__ == '__main__':
    # Set the logging to write to terminal AND file
    logging.basicConfig(
        level=logging.INFO,
        filename='./prep.log',
        filemode='w'
    )
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logging.info('Executing fullsurvey.py as file')
    logging.info('*** THIS IS AN INSTANT-FEEDBACK SIMULATION')

    # Get a cursor
    # TODO: Correct package imports & references
    logging.debug('Getting connection')
    conn = get_connection()
    cursor = conn.cursor()
    # Execute the simulation based on command-line arguments
    logging.debug('Doing update function')
    update(cursor)
