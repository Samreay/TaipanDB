from src.scripts import create
from src.resources.v0_0_1.ingest import loadGuides, loadScience, \
    loadCentroids, loadStandards
from src.resources.v0_0_1.manipulate import makeScienceDiff, makeTargetPosn

from src.resources.v0_0_1.insert.insertAlmanac import execute as iAexec

from src.resources.v0_0_1.readout.readCentroids import execute as rCexec
from src.resources.v0_0_1.readout.readScienceTypes import execute as \
    rScTyexec

from src.resources.v0_0_1.manipulate.makeScienceTypes import execute as \
    mScTyexec
from src.resources.v0_0_1.manipulate.makeSciencePriorities import execute as \
    mScPexec

from taipan.scheduling import Almanac, DarkAlmanac
from taipan.simulate.logic import compute_target_types, \
    compute_target_priorities_tree

from src.scripts.connection import get_connection

import os
import datetime
import logging
import sys
import traceback


# Define the fields we want to make indices for
TABLE_INDICES = {
    'target': [
        'is_standard',
        'is_guide',
        'is_science',
        'is_active',
    ],
    'science_target': [
        'done',
        'success',
        'is_lowz_target',
    ],
    'target_field': [
        'tile_pk',
    ],
    'target_posn': [
        'target_id',
        'field_id',
    ],
    'tile': [
        'field_id',
        'is_queued',
        'is_observed',
    ],
    'tiling_config': [
        'date_config',
        'date_obs',
    ],
    'tiling_info': [
        'tile_pk',
        'field_id',
    ],
    'observability': [
        # 'date',
        # 'field_id',
        # 'airmass',
        # 'sun_alt',
        ['field_id', 'airmass', ],
        ['field_id', 'airmass', 'date', ],
    ]
}


def generate_indices(cursor):
    logging.info('Generating table indices')
    for tab, fields in TABLE_INDICES.items():
        for field in fields:
            logging.info('Starting to index table %s on field %s' %
                         (tab, field, ))
            create.create_index(cursor, tab,
                                [field, ] if not isinstance(field, list)
                                else field,
                                ordering=None)
    logging.info('Indexing complete!')
    return


def update(cursor):
    # resource_dir = os.path.dirname(__file__) + os.sep + "v0_0_1" + os.sep
    # data_dir = "/data/resources/0.0.1/"
    # # data_dir = "/Users/marc/Documents/taipan/tiling-code/TaipanCatalogues/"
    # table_dir = resource_dir + os.sep + "tables"
    # # table_dir = '/data/resources/tables_to_replace'
    #
    # # # Clear out the targets table
    # # logging.info('Removing existing target catalogues')
    # # cursor.execute('DELETE FROM target')
    # # # Destroy the existing science_targets table
    # # logging.info('Removing science table')
    # # cursor.execute('DROP TABLE science_target')
    #
    # create.create_tables(cursor, table_dir)
    #
    # fields_file = data_dir + "pointing_centers.radec"
    # loadCentroids.execute(cursor, fields_file=fields_file)
    #
    # # guides_file = data_dir + "SCOSxAllWISE.photometry.forTAIPAN." \
    #                          # "reduced.guides_nodups.fits"
    # # guides_file = data_dir + 'guides_UCAC4_btrim.fits'
    # guides_file = data_dir + 'random_mock_guides_160930.fits'
    # loadGuides.execute(cursor, guides_file=guides_file)
    #
    # # standards_file = data_dir + 'SCOSxAllWISE.photometry.forTAIPAN.' \
    # #                             'reduced.standards_nodups.fits'
    # standards_file = data_dir + 'random_mock_standards_160928.fits'
    # loadStandards.execute(cursor, standards_file=standards_file)
    #
    # # # science_file = data_dir + 'priority_science.v0.101_20160331.fits'
    # # science_file = data_dir + 'Taipan_mock_inputcat_v1.1_170208.fits'
    # # science_file = data_dir + 'Taipan_mock_inputcat_v1.2_170303.fits'
    # # science_file = data_dir + 'Taipan_mock_inputcat_v1.3_170504.fits'
    # science_file = data_dir + 'Taipan_mock_inputcat_v2.0_170518.fits'
    # loadScience.execute(cursor, science_file=science_file)
    # #
    # # # Commit here in case something further along fails
    # # logging.info('Committing raw target information...')
    # # cursor.connection.commit()
    # # logging.info('...done!')
    #
    #
    # logging.info('Computing target-field relationships...')
    # makeTargetPosn.execute(cursor, do_guides=True, do_standards=True)
    #
    # # Commit again
    # logging.info('Committing computed target information...')
    # cursor.connection.commit()
    # logging.info('...done!')
    #
    # # Compute target priorities and types
    # target_types_init = rScTyexec(cursor)
    # # Compute and store target types
    # # Do it in batches of 200,000 to avoid overloading the VM memory
    # batch_size = 200000
    # i = 0
    # while i < len(target_types_init):
    #     tgt_types = compute_target_types(target_types_init[i:i+batch_size],
    #                                      prisci=True)
    #     mScTyexec(cursor, tgt_types['target_id'], tgt_types['is_h0_target'],
    #               tgt_types['is_vpec_target'], tgt_types['is_lowz_target'])
    #     i += batch_size
    #
    # target_types = rScTyexec(cursor)
    # i = 0
    # while i < len(target_types):
    #     # Compute and store priorities
    #     # Need back consistent, up-to-date types, so read back what we just put
    #     # into the database
    #     priorities = compute_target_priorities_tree(
    #         target_types[i:i+batch_size],
    #         default_priority=0,
    #         prisci=True)
    #     mScPexec(cursor, target_types[i:i+batch_size]['target_id'], priorities)
    #     i += batch_size
    #
    # # logging.info('Computing target difficulties...')
    # # makeScienceDiff.execute(cursor)
    #
    # # Commit again
    # logging.info('Committing target type/priority information...')
    # cursor.connection.commit()
    # logging.info('...done!')

    # # Instantiate the Almanacs
    # sim_start = datetime.date(2017, 4, 1)
    # sim_end = datetime.date(2027, 4, 1)
    # global_start = datetime.datetime.now()
    #
    # fields = rCexec(cursor)
    #
    # logging.info('Generating dark almanac...')
    # dark_alm = DarkAlmanac(sim_start, end_date=sim_end)
    # logging.info('Done!')
    #
    # i = 1
    # for field in fields:
    #     almanac = Almanac(field.ra, field.dec, sim_start, end_date=sim_end,
    #                       minimum_airmass=2.0, populate=True, resolution=15.)
    #     logging.info('Computed almanac %5d / %5d' % (i, len(fields),))
    #     iAexec(cursor, field.field_id, almanac, dark_almanac=dark_alm)
    #     # Commit after every Almanac due to the expense of computing
    #     cursor.connection.commit()
    #     logging.info('Inserted almanac %5d / %5d' % (i, len(fields),))
    #     i += 1
    #
    #
    # cursor.connection.commit()

    # Create the table indices
    generate_indices(cursor)
    cursor.connection.commit()

    return


if __name__ == '__main__':
    # Override the sys.excepthook behaviour to log any errors
    # http://stackoverflow.com/questions/6234405/logging-uncaught-exceptions-in-python
    def excepthook_override(exctype, value, tb):
        # logging.error(
        #     'My Error Information\nType: %s\nValue: %s\nTraceback: %s' %
        #     (exctype, value, traceback.print_tb(tb), ))
        # logging.error('Uncaught error/exception detected',
        #               exctype=(exctype, value, tb))
        logging.critical(''.join(traceback.format_tb(tb)))
        logging.critical('{0}: {1}'.format(exctype, value))
        # logging.error('Type:', exctype)
        # logging.error('Value:', value)
        # logging.error('Traceback:', tb)
        return


    sys.excepthook = excepthook_override

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
    logging.info('*** COMMENCING DATABASE PREP')

    # Get a cursor
    # TODO: Correct package imports & references
    logging.debug('Getting connection')
    conn = get_connection()
    cursor = conn.cursor()
    # Execute the simulation based on command-line arguments
    logging.debug('Doing update function')
    update(cursor)
