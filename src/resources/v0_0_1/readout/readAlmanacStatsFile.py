import logging
import datetime
import sys
import os
import traceback
import re
import numpy as np
import psycopg2

from src.resources.v0_0_1.readout import readCentroids as rC
from src.resources.v0_0_1.insert import insertAlmanac as iA
from src.scripts.connection import get_connection
from src.scripts.create import create_child_table, create_index

from taipan.scheduling import Almanac, DarkAlmanac

import multiprocessing
from functools import partial

ALMANAC_FILE_LOC = '/data/resources/0.0.1/alms/'

OBS_CHILD_CHUNK_SIZE = 200
MAX_TABLES = 20000


def obs_child_table_name(field_id, chunk_size=OBS_CHILD_CHUNK_SIZE):
    low_val = field_id - (field_id % chunk_size)
    high_val = low_val + chunk_size
    return 'obs_%06d_to_%06d' % (low_val, high_val, )


def load_almanac_partition(field,
                           # cursor=get_connection().cursor(),
                           alm_file_path=ALMANAC_FILE_LOC,
                           resolution=15.,
                           minimum_airmass=2.0,
                           datetime_from=datetime.date.today(),
                           datetime_to=datetime.date.today() +
                                       datetime.timedelta(1)):
    # Read the almanac in from file
    with get_connection().cursor() as cursor_int:
        # Read-in the almanac. If it doesn't exist, create it
        # Load an Almanac - we'll change the dates later
        almanac = Almanac(field.ra, field.dec, datetime.date.today(),
                          observing_period=1,
                          resolution=resolution,
                          minimum_airmass=minimum_airmass,
                          populate=False, alm_file_path=alm_file_path)
        # Examine the almanancs directory for an almanac for this field
        match = False
        # logging.info('Looking for a matching almanac in %s' % alm_file_path)
        possible_matches = [_ for _ in os.listdir(alm_file_path) if
                            'R%.1f_D%.1f' % (field.ra, field.dec,) in _]
        # logging.info('Possible file matches for field %6d: %d' % (
        #     field.field_id, len(possible_matches)))
        if len(possible_matches) > 0:
            i = 0
            while i < len(possible_matches) and not match:
                startre = re.compile(r'start(?P<sd>[0-9]{6})')
                endre = re.compile(r'end(?P<ed>[0-9]{6})')
                startmatch = startre.search(possible_matches[i])
                endmatch = endre.search(possible_matches[i])
                # logging.info('Matches found: %s, %s' % (startmatch.group('sd'),
                #                                         endmatch.group('ed')))
                if startmatch and endmatch:
                    sd = datetime.datetime.strptime(
                        startmatch.group('sd'), '%y%m%d').date()
                    ed = datetime.datetime.strptime(
                        endmatch.group('ed'), '%y%m%d').date()
                    # logging.info('Parsed start date %s, end date %s' % (
                    #     sd.strftime('%y%m%d'), ed.strftime('%y%m%d')
                    # ))
                    if sd <= datetime_from and ed >= datetime_to:
                        match = True
                if not match:
                    i += 1

        if match:
            # sd and ed will still be in memory from the match being made
            almanac.start_date = sd
            almanac.end_date = ed
            logging.info('Loading almanac for field %6d' % field.field_id)
            check = almanac.load(filepath=alm_file_path)

        if not match or not check:
            sd = datetime_from
            almanac.start_date = sd
            ed = datetime_to
            almanac.end_date = ed
            logging.info('Computing almanac for field %6d' % field.field_id)
            almanac.generate_almanac_bruteforce()

        logging.info('Sourcing dark almanac')
        # Either grab or make the dark almanac
        dark_alm = DarkAlmanac(sd, end_date=ed, resolution=15.,
                               populate=True, alm_file_path=alm_file_path)

        # This code chunk creates a child for each field
        # Turns out PSQL can't handle this as yet
        # try:
        #     # Create the relevant child table
        #     child_table_name = 'obs_%06d' % field.field_id
        #     logging.info('Creating table %s' % child_table_name)
        #     create_child_table(cursor_int, child_table_name,
        #                        'observability',
        #                        check_conds=[
        #                            ('field_id', '=', field.field_id),
        #                        ],
        #                        primary_key=['field_id', 'date', ])
        #     # Insert the data points into the child table
        #     logging.info('Populating table %s' % child_table_name)
        #     iA.execute(cursor_int, field.field_id, almanac, dark_almanac=dark_alm,
        #                update=None, target_table=child_table_name)
        #     # Create the necessary indices
        #     logging.info('Creating indices on %s' % child_table_name)
        #     create_index(cursor_int, child_table_name, ['date', ])
        #     create_index(cursor_int, child_table_name, ['field', ])
        #     create_index(cursor_int, child_table_name, ['field_id', 'date'])
        #     create_index(cursor_int, child_table_name, ['date', 'airmass'])
        #     create_index(cursor_int, child_table_name, ['date', 'sun_alt'])
        # except (psycopg2.ProgrammingError, psycopg2.OperationalError) as e:
        #     # Something is wrong, undo what this cursor tried to do
        #     logging.info('Table already exists for field %d' % field.field_id)
        #     cursor_int.connection.rollback()

        # This code block does things based on splitting it into chunks
        # of OBS_CHILD_CHUNK_SIZE fields
        try:
            child_table_name = obs_child_table_name(field.field_id)
            iA.execute(cursor_int, field.field_id, almanac,
                       dark_almanac=dark_alm, update=None,
                       target_table=child_table_name)
        except (psycopg2.ProgrammingError, psycopg2.OperationalError) as e:
            logging.info('Insertion error for field %d' %
                         field.field_id)



    return


def make_almanac_n(field, sim_start=datetime.datetime.now(),
                   sim_end=datetime.datetime.now() + datetime.timedelta(1),
                   minimum_airmass=2.0,
                   resolution=15.):
    almanac = Almanac(field.ra, field.dec, sim_start, end_date=sim_end,
                      minimum_airmass=minimum_airmass, populate=True,
                      resolution=resolution)
    logging.info('Computed almanac for field %5d' % (field.field_id, ))
    almanac.save(filepath=ALMANAC_FILE_LOC)
    logging.info('Saved almanac for field %5d' % (field.field_id,))
    return


def create_almanac_files(cursor, sim_start, sim_end,
                         resolution=15., minimum_airmass=2.0,
                         active_only=False):
    """
    Compute Almanacs from the fields in the database and save them to file
    """

    fields = rC.execute(cursor, active_only=active_only)

    # Farm out the creation of the Almanacs
    make_almanac_n_partial = partial(make_almanac_n,
                                     sim_start=sim_start, sim_end=sim_end,
                                     minimum_airmass=minimum_airmass,
                                     resolution=resolution)

    pool = multiprocessing.Pool(processes=multiprocessing.cpu_count() - 1)
    _ = pool.map(make_almanac_n_partial, fields)
    pool.close()
    pool.join()

    return


def next_observable_period(cursor, field_id, datetime_from, datetime_to=None,
                           minimum_airmass=2.0, dark=True, grey=False,
                           alm_dir=ALMANAC_FILE_LOC, resolution=15.):
    """
    Determine the next period when the field is observable, based on airmass
    only (i.e. don't look at day/night, or dark status). Arguments and return
    are the same
    for readAlmanacStats.next_observable_period; however, this is the pure
    Python (i.e. database-free) implementation.
    """
    if dark and grey:
        raise ValueError("Can't simultaneously request dark and grey time")

    if datetime_to is None:
        raise ValueError("datetime_to can't be none for file version of "
                         "next_observable_period")

    # Read in the field information from the database
    field = rC.execute(cursor, field_ids=[field_id, ], active_only=False)

    # Read-in the almanac. If it doesn't exist, create it
    # Load an Almanac - we'll change the dates later
    almanac = Almanac(field.ra, field.dec, datetime.date.today(),
                      observing_period=1,
                      resolution=resolution, minimum_airmass=minimum_airmass,
                      populate=False)
    # Examine the almanancs directory for an almanac for this field
    match = False
    possible_matches = [_ for _ in os.listdir(alm_dir) if
                        'R%.1f_D%.1f' % (field.ra, field.dec, ) in _]
    if len(possible_matches) > 0:
        i = 0
        while i < len(possible_matches) and not match:
            startre = re.compile(r'start[0-9]{6}')
            endre = re.compile(r'end[0-9]{6}')
            startmatch = startre.match(possible_matches[i])
            endmatch = endre.match(possible_matches[i])
            if startmatch and endmatch:
                sd = datetime.datetime.strptime(
                    startmatch.group(0), '%y%m%d').date()
                ed = datetime.datetime.strptime(
                    endmatch.group(0), '%y%m%d').date()
                if sd <= datetime_from.date() and ed > datetime_to.date():
                    match = True
            if not match:
                i += 1

    if match:
        # sd and ed will still be in memory from the match being made
        almanac.start_date = sd
        almanac.end_date = ed
        check = almanac.load(filepath=alm_dir)

    if not match or not check:
        sd = datetime_from.date()
        almanac.start_date = sd
        ed = datetime_to.date() + datetime.timedelta(1)
        almanac.end_date = ed
        almanac.generate_almanac_bruteforce()

    # Either grab or make the dark almanac
    dark_alm = DarkAlmanac(sd, end_date=ed, resolution=15.,
                           populate=True, alm_file_path=alm_dir)

    # Perform the calculation
    per = almanac.next_observable_period(datetime_from, datetime_to=datetime_to,
                                         minimum_airmass=minimum_airmass,
                                         )
    if per[0] is None or (not dark and not grey):
        return per

    dark_datum_start = np.argwhere(np.logical_and(
        dark_alm.data['date'] >= per[0] - datetime.timedelta(
            minutes=resolution),
        dark_alm.data['date'] <= per[0] + datetime.timedelta(
            minutes=resolution)
    ))[0]
    if per[1] is None:
        dark_datum_end = dark_alm.data['dark_time'][-1]
    else:
        dark_datum_end = np.argwhere(np.logical_and(
            dark_alm.data['date'] >= per[0] - datetime.timedelta(
                minutes=resolution),
            dark_alm.data['date'] <= per[0] + datetime.timedelta(
                minutes=resolution)
        ))[0]

    # By this point, exactly one of dark or grey must be True (and hence,
    # the other one False)
    if dark_alm.data['dark_time'][dark_datum_start] == dark and \
                    dark_alm.data['dark_time'][dark_datum_end] == dark:
        return per

    if dark_alm.data['dark_time'][dark_datum_start] != dark:
        try:
            per[0] = dark_alm.data[np.logical_and(
                np.logical_and(dark_alm.data['date'] > per[0],
                               dark_alm.data['date'] < per[1]),
                dark_alm.data['dark_time'] == dark
            )]['date'][0]
        except IndexError:
            return (None, None, )

    if dark_alm.data['dark_time'][dark_datum_end] != dark:
        per[1] = dark_alm.data[np.logical_and(
            dark_alm.data['date'] > per[0],
            dark_alm.data['dark_time'] != dark
        )]['date'][-1]
        # This must exist if per[0] exists and the if test fails

    return per

def hours_observable(cursor, field_id, datetime_from, datetime_to,
                     exclude_grey_time=True, exclude_dark_time=False,
                     minimum_airmass=2.0, hours_better=True,
                     resolution=15., airmass_delta=0.05):
    """
    Determine the number of hours observable from Python-pickled Almanacs.
    Parameters
    ----------
    cursor
    field_id
    datetime_from
    datetime_to
    exclude_grey_time
    exclude_dark_time
    minimum_airmass
    hours_better
    resolution
    airmass_delta

    Returns
    -------

    """
    pass


if __name__ == '__main__':

    def load_almanacs_partition_all(cursor):
        logging.info('Loading almanacs from file to DB')
        fields = rC.execute(cursor, active_only=False)

        # for field in fields:
        #     load_almanac_partition(field, datetime_from=sim_start,
        #                            datetime_to=sim_end,
        #                            resolution=15.,
        #                            minimum_airmass=2.0)

        load_almanac_partition_partial = partial(load_almanac_partition,
                                                 # cursor=cursor,
                                                 datetime_from=sim_start,
                                                 datetime_to=sim_end,
                                                 resolution=15.,
                                                 minimum_airmass=2.0)

        pool = multiprocessing.Pool(processes=
                                    multiprocessing.cpu_count() -
                                    1)
        _ = pool.map(load_almanac_partition_partial, fields)
        pool.close()
        pool.join()

        logging.info('Loading of all Almanacs from file complete!')

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
        level=logging.WARNING,
        filename='./loadlog_rASF.log',
        filemode='w'
    )
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    console = logging.StreamHandler()

    sim_start = datetime.date(2017, 4, 1)
    sim_end = datetime.date(2024, 1, 1)

    cursor_master = get_connection().cursor()

    # Create the child tables
    for i in range(1, MAX_TABLES, OBS_CHILD_CHUNK_SIZE):
        create_child_table(cursor_master,
                           obs_child_table_name(i), 'observability',
                           check_conds=[
                               ('field_id', '>', i-1),
                               ('field_id', '<', i+OBS_CHILD_CHUNK_SIZE)],
                           primary_key=['field_id', 'date', ])
        logging.info('Created table %s' % obs_child_table_name(i))


    # Generate file almanacs

    # logging.info('Generating dark almanac...')
    # dark_alm = DarkAlmanac(sim_start, end_date=sim_end)
    # dark_alm.save(filepath=ALMANAC_FILE_LOC)
    # logging.info('Done!')
    #
    # logging.info('Generating standard almanacs...')
    # create_almanac_files(cursor_master, sim_start, sim_end, resolution=15.,
    #                      minimum_airmass=2.0, active_only=False)
    # logging.info('...done!')

    # Insert file almanacs into partitioned database
    load_almanacs_partition_all(cursor_master)

    # Create the necessary indices
    for i in range(1, MAX_TABLES, OBS_CHILD_CHUNK_SIZE):
        child_table_name = obs_child_table_name(i)
        logging.info('Creating indices on %s' % child_table_name)
        create_index(cursor_master, child_table_name, ['date', ])
        create_index(cursor_master, child_table_name, ['field', ])
        # create_index(cursor_int, child_table_name, ['field_id', 'date'])
        create_index(cursor_master, child_table_name, ['date', 'airmass'])
        create_index(cursor_master, child_table_name, ['date', 'sun_alt'])
