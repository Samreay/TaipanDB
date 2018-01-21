from taipandb.scripts import create
from taipandb.resources.v0_0_1.ingest import loadGuides, loadScience, \
    loadCentroids, loadStandards, loadSkies
from taipandb.resources.v0_0_1.manipulate import makeScienceDiff, makeTargetPosn

from taipandb.resources.v0_0_1.insert.insertAlmanac import execute as iAexec

from taipandb.resources.v0_0_1.readout.readCentroids import execute as rCexec
from taipandb.resources.v0_0_1.readout.readScienceTypes import execute as \
    rScTyexec

from taipandb.resources.v0_0_1.manipulate.makeScienceTypes import execute as \
    mScTyexec
from taipandb.resources.v0_0_1.manipulate.makeSciencePriorities import execute as \
    mScPexec

from taipan.scheduling import Almanac, DarkAlmanac
from taipan.simulate.logic import compute_target_types, \
    compute_target_priorities_tree

from taipandb.scripts.connection import get_connection

import os
import datetime
import logging
import sys
import traceback
import multiprocessing
from functools import partial

OBS_CHILD_CHUNK_SIZE = 200
"""Number of fields per child table of ``observability``"""
MAX_FIELDS = 20000
"""The maximum expected number of fields in the database"""

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
    'field': [
        'is_active',
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
    # 'observability': [
    #     'date',
    #     'field_id',
    #     ['date', 'airmass', ],
    #     ['field_id', 'date', 'airmass', ],
    #     ['field_id', 'dark', 'airmass', 'date', ],
    #     ['field_id', 'dark', 'airmass', 'sun_alt', 'date', ],
    # ]
}
"""Dictionary of indices to be created on tables

For each table (key), an index will be created based on the column(s) in 
each element of the index list (value)."""


def obs_child_table_name(field_id, chunk_size=OBS_CHILD_CHUNK_SIZE):
    """
    Auto-generate the name of a child table of ``observability``

    This functions takes a field ID and the chunk size of the current
    database setup, and returns the name of the child table of
    the ``observability`` table that this field's Almanac data may be found in.

    Parameters
    ----------
    field_id : :obj:`int`
        Field ID
    chunk_size : :obj:`int`
        The field ID range of each child table of ``observability``. Defaults
        to :any:`OBS_CHILD_CHUNK_SIZE`.

    Returns
    -------
    :obj:`str`
        The name of the relevant child table.
    """
    low_val = field_id - ((field_id - 1) % chunk_size)
    high_val = low_val + chunk_size - 1
    return 'obs_%06d_to_%06d' % (low_val, high_val, )


def generate_indices(cursor):
    """
    Generate indices on each table, based on :any:`TABLE_INDICES`

    For each table name referenced by a key of :any:`TABLE_INDICES`, and index
    will be created on each element of the matching dictionary value.

    Parameters
    ----------
    cursor : :obj:`psycopg2.connection.cursor`
        Cursor for communicating with the database
    """
    logging.info('Generating table indices')
    for tab, fields in TABLE_INDICES.items():
        if tab != 'observability':
            for field in fields:
                logging.info('Starting to index table %s on field %s' %
                             (tab, field, ))
                create.create_index(cursor, tab,
                                    [field, ] if not isinstance(field, list)
                                    else field,
                                    ordering=None)
        else:
            for i in range(1, MAX_FIELDS, OBS_CHILD_CHUNK_SIZE):
                child_table_name = obs_child_table_name(i)
                for field in fields:
                    logging.info('Starting to index table %s on field %s' %
                                 (child_table_name, field,))
                    create.create_index(cursor, child_table_name,
                                        [field, ] if not isinstance(field, list)
                                        else field,
                                        ordering=None)
                cursor.execute(
                    'CLUSTER %s USING %s_pkey' % (child_table_name,
                                                  child_table_name,))
                create.vacuum_analyze(cursor, table=child_table_name)

    logging.info('Indexing complete!')
    return


def make_almanac_n(field, sim_start=None, sim_end=None, dark_alm=None):
    """
    Helper function for multithreaded entry of Almanac/``observability`` data

    Parameters
    ----------
    field : :obj:`taipan.core.TaipanTile`
        Representation of field to enter data for
    sim_start : :obj:`datetime.date`
        Earliest date for data generation
    sim_end : :obj:`datetime.date`
        Latest date for data generation
    dark_alm : :obj:`taipan.scheduling.DarkAlmanac`
        Associated :any:`DarkAlmanac` object
    """
    # Multiprocessing needs a fresh cursor per instance
    with get_connection().cursor() as cursor:
        almanac = Almanac(field.ra, field.dec, sim_start, end_date=sim_end,
                          minimum_airmass=2.0, populate=True, resolution=15.)
        logging.info('Computed almanac for field %5d' % (field.field_id, ))
        iAexec(cursor, field.field_id, almanac, dark_almanac=dark_alm,
               target_table=obs_child_table_name(field.field_id))
        # Commit after every Almanac due to the expense of computing
        cursor.connection.commit()
        logging.info('Inserted almanac for field %5d' % (field.field_id,))

def update(cursor):
    """
    Initialize the database for a run.

    .. note::
        This function will typically take about 24-36 hours to run. We
        strongly recommend you run the source code file as a script.

    This function takes a *clean* database instance, and does the following
    steps:

    - Create the tables
    - Load all target types into the database
    - Compute target-field relationships for all targets, and insert into
      the database
    - Compute initial target priorities and types for science targets, and
      insert this information into the database
    - Generate (or load from file) Almanacs for each field, and write to the
      database
    - Create indices for tables, as per :any:`TABLE_INDICES`

    Parameters
    ----------
    cursor : :obj:`psycopg2.connection.cursor`
        Cursor for communicating with the database.
    """
    resource_dir = os.path.dirname(__file__) + os.sep + "v0_0_1" + os.sep
    data_dir = "/data/resources/0.0.1/"
    # data_dir = "/Users/marc/Documents/taipan/tiling-code/TaipanCatalogues/"
    # table_dir = resource_dir + os.sep + "tables"
    table_dir = '/data/resources/tables_to_replace'

    # Clear out the targets table
    logging.info('Removing existing target catalogues')
    cursor.execute('DELETE FROM target')
    # Destroy the existing science_targets table
    # logging.info('Removing science table')
    # cursor.execute('DROP TABLE science_target')
    logging.info('Remove any existing tiles')
    cursor.execute('DELETE FROM tile')

    # create.create_tables(cursor, table_dir)
    #
    # # fields_file = data_dir + "pointing_centers.radec"
    # # loadCentroids.execute(cursor, fields_file=fields_file)
    # # fields_file_fullsurvey = data_dir + "pointing_centers_fullsurvey.radec"
    # # loadCentroids.execute(cursor, fields_file=fields_file_fullsurvey,
    # #                       mark_active=False)

    guides_file = data_dir + "SCOSxAllWISE.photometry.forTAIPAN." \
                             "reduced.guides_nodups.fits"
    guides_file = data_dir + 'guides_UCAC4_btrim.fits'
    guides_file = data_dir + 'Guide_UCAC4.fits'
    loadGuides.execute(cursor, guides_file=guides_file)

    # standards_file = data_dir + 'SCOSxAllWISE.photometry.forTAIPAN.' \
    #                             'reduced.standards_nodups.fits'
    standards_file = data_dir + 'Fstar_Panstarrs.fits'
    loadStandards.execute(cursor, standards_file=standards_file)
    standards_file = data_dir + 'Fstar_skymapperdr1.fits'
    loadStandards.execute(cursor, standards_file=standards_file)

    sky_file = data_dir + 'skyfibers_v17_gaia_ucac4_final_fix.fits'
    loadSkies.execute(cursor, skies_file=sky_file)

    # # science_file = data_dir + 'priority_science.v0.101_20160331.fits'
    # science_file = data_dir + 'Taipan_mock_inputcat_v1.1_170208.fits'
    # science_file = data_dir + 'Taipan_mock_inputcat_v1.2_170303.fits'
    # science_file = data_dir + 'Taipan_mock_inputcat_v1.3_170504.fits'
    # science_file = data_dir + 'Taipan_mock_inputcat_v2.0_170518.fits'
    # science_file = data_dir + 'Taipan_InputCat_v0.3_20170731.fits'
    # science_file = data_dir + 'Taipan_InputCat_v0.35_20170831.fits'
    science_file = data_dir + 'wsu_targetCatalog.fits'
    loadScience.execute(cursor, science_file=science_file)
    #
    # Commit here in case something further along fails
    logging.info('Committing raw target information...')
    cursor.connection.commit()
    logging.info('...done!')

    # If using the SPT catalogue, we want to:
    # - Compute difficulties now;
    # - Remove any targets above a certain difficulty threshold;
    # - Disable any fields outside the SPT area
    if science_file == data_dir + 'wsu_targetCatalog.fits':
        cursor.execute('UPDATE field SET is_active=False WHERE '
                       'dec > -45. OR dec < -65 OR (ra < 330. AND ra > 15.)')
        cursor.execute('DELETE FROM target WHERE (dec > -45. OR dec < -65. OR '
                       '(ra < 330. AND RA > 15.)) AND target_id >= 0')

        logging.info('Computing target difficulties...')
        makeScienceDiff.execute(cursor)
        cursor.connection.commit()

        # Manually execute the necessary DB commands for restricting the
        # targets and fields
        cursor.execute('DELETE FROM target WHERE target_id IN '
                       '(SELECT target_id FROM science_target WHERE '
                       'difficulty > 2000)')

    #
    #
    logging.info('Computing target-field relationships...')
    makeTargetPosn.execute(cursor,
                           do_guides=True,
                           do_standards=True,
                           do_skies=True,
                           active_only=True,
                           parallel_workers=7)
    #
    # Commit again
    logging.info('Committing computed target information...')
    cursor.connection.commit()
    logging.info('...done!')

    # Compute target priorities and types
    target_types_init = rScTyexec(cursor)
    # Compute and store target types
    # Do it in batches of 200,000 to avoid overloading the VM memory
    batch_size = 200000
    i = 0
    while i < len(target_types_init):
        tgt_types = compute_target_types(target_types_init[i:i+batch_size],
                                         prisci=True)
        mScTyexec(cursor, tgt_types['target_id'], tgt_types['is_h0_target'],
                  tgt_types['is_vpec_target'], tgt_types['is_lowz_target'])
        i += batch_size

    target_types = rScTyexec(cursor)
    i = 0
    while i < len(target_types):
        # Compute and store priorities
        # Need back consistent, up-to-date types, so read back what we just put
        # into the database
        priorities = compute_target_priorities_tree(
            target_types[i:i+batch_size],
            default_priority=0,
            prisci=True)
        mScPexec(cursor, target_types[i:i+batch_size]['target_id'], priorities)
        i += batch_size

    if science_file != data_dir + 'wsu_targetCatalog.fits':
        logging.info('Computing target difficulties...')
        makeScienceDiff.execute(cursor)

    # Commit again
    logging.info('Committing target type/priority information...')
    cursor.connection.commit()
    logging.info('...done!')

    # # Instantiate the Almanacs
    # sim_start = datetime.date(2017, 4, 1)
    # sim_end = datetime.date(2024, 1, 1)
    # global_start = datetime.datetime.now()

    # # Create the child tables
    # logging.info('Creating observability child tables')
    # for i in range(1, MAX_FIELDS, OBS_CHILD_CHUNK_SIZE):
    #     child_table_name = obs_child_table_name(i)
    #     create.create_child_table(cursor,
    #                               child_table_name, 'observability',
    #                               check_conds=[
    #                                   ('field_id', '>', i - 1),
    #                                   ('field_id', '<',
    #                                    i + OBS_CHILD_CHUNK_SIZE)],
    #                               primary_key=['field_id', 'date', ])
    #     logging.debug('Created table %s' % child_table_name)

    #
    # fields = rCexec(cursor, active_only=False)
    #
    # logging.info('Generating dark almanac...')
    # dark_alm = DarkAlmanac(sim_start, end_date=sim_end)
    # logging.info('Done!')
    #
    # # i = 1
    # # for field in fields:
    # #     almanac = Almanac(field.ra, field.dec, sim_start, end_date=sim_end,
    # #                       minimum_airmass=2.0, populate=True, resolution=15.)
    # #     logging.info('Computed almanac %5d / %5d' % (i, len(fields),))
    # #     iAexec(cursor, field.field_id, almanac, dark_almanac=dark_alm)
    # #     # Commit after every Almanac due to the expense of computing
    # #     cursor.connection.commit()
    # #     logging.info('Inserted almanac %5d / %5d' % (i, len(fields),))
    # #     i += 1
    #
    # # 170619 - Use multiprocessing to speed this up (hopefully)
    # make_almanac_n_partial = partial(make_almanac_n,
    #                                  sim_start=sim_start, sim_end=sim_end,
    #                                  dark_alm=dark_alm)
    # pool = multiprocessing.Pool(8)
    # pool.map(make_almanac_n_partial, fields)
    # pool.close()
    # pool.join()
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
