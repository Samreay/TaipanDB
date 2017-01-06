# Insert calculated almanacs into the database

import logging
# from taipan.core import TaipanTile
from taipan.scheduling import Almanac, DarkAlmanac, ephem_to_dt, localize_utc_dt
from src.scripts.create import insert_many_rows
from src.scripts.manipulate import upsert_many_rows
# from src.scripts.extract import extract_from, extract_from_joined
import numpy as np

from src.resources.v0_0_1.readout.readCentroids import execute as rCexec

from src.scripts.connection import get_connection

import datetime
import sys

from src.resources.v0_0_1.readout import readScience as rSc


# def compute_sci_targets_complete(cursor, tile, tgt_list):
#     """
#     Compute the number of science targets remaining in a particular field
#
#     Parameters
#     ----------
#     tile
#     tgt_list
#
#     Returns
#     -------
#
#     """
#
#     # Get the list of targets
#     query_result = rSc.execute(cursor)


def execute(cursor, field_id, almanac, dark_almanac=None, update=None):
    """
    Insert the given almanac into the database.

    Parameters
    ----------
    cursor:
        psycopg2 cursor for interacting with the database
    field_id:
        The field_id for the almanac to be written. This field must already
        exist in the field table.
    almanac:
        The taipan.scheduling.Almanac object to write to the database.
    dark_almanac:
        Optional; the dark_almanac corresponding to the almanac passed. Needed
        to put in sun_alt, is_dark etc. values. Defaults to None, at which
        point a new corresponding DarkAlmanac will be generated. Note that a
        fresh DarkAlmanac will also be generated if the passed DarkAlmanac does
        not exactly match the almanac in terms of start_date, end_date and
        resolution.
    update:
        Optional; Boolean value denoting whether to update any almanac data
        points already existing in the database (True) or simply not write
        new values talmo those points (False), or try to blindly insert the
        values into the DB. Defaults to False. Using True
        should only be necessary if, e.g. fields have changed. Using None will
        throw a ProgrammingError if an identical table row already exists.

    Returns
    -------
    Nil. Almanacs are written directly into the database.
    """
    logging.info('Inserting almanacs into database...')

    # Input checking
    gen_dark_alm = False
    if dark_almanac is not None:
        if dark_almanac.start_date != almanac.start_date or \
                        dark_almanac.end_date != almanac.end_date or \
                        dark_almanac.resolution != almanac.resolution:
            gen_dark_alm = True
    else:
        gen_dark_alm = True
    if gen_dark_alm:
        dark_almanac = DarkAlmanac(almanac.start_date,
                                   end_date=almanac.end_date,
                                   resolution=almanac.resolution)

    # Format the almanac data into something that can inserted into the database
    # data_out = [(field_id, localize_utc_dt(ephem_to_dt(dt)),
    #              almanac.airmass[dt], dark_almanac.sun_alt[dt],
    #              bool(dark_almanac.dark_time[dt])) for
    #             dt in almanac.airmass.keys()]
    data_out = np.asarray([[field_id] * almanac.data['date'].shape[-1],
                           map(lambda x: ephem_to_dt(x),
                               almanac.data['date']),
                           almanac.data['airmass'],
                           dark_almanac.data['sun_alt'],
                           dark_almanac.data['dark_time']]).T
    logging.debug(data_out)
    logging.debug(data_out.shape)
    logging.debug(data_out[:, 1].shape)
    logging.debug(len(set(data_out[:, 1])))

    # Write the data to the db
    if update is None:
        insert_many_rows(cursor, 'observability',
                         data_out,
                         columns=['field_id', 'date', 'airmass',
                                  'sun_alt', 'dark'],
                         skip_on_conflict=False)
    elif not update:
        insert_many_rows(cursor, 'observability',
                         data_out,
                         columns=['field_id', 'date', 'airmass',
                                  'sun_alt', 'dark'],
                         skip_on_conflict=True)
    else:
        upsert_many_rows(cursor, 'observability',
                         data_out,
                         columns=['field_id', 'date', 'airmass',
                                  'sun_alt', 'dark'])

    logging.info('Insertion of almanac completed')

    return

if __name__ == '__main__':
    # Use this module as a script to generate Almanac data for all fields
    # currently existing in the database
    # Get a cursor

    sim_start = datetime.date(2017, 4, 1)
    sim_end = datetime.date(2027, 4, 1)
    global_start = datetime.datetime.now()

    # Override the sys.excepthook behaviour to log any errors
    # http://stackoverflow.com/questions/6234405/logging-uncaught-exceptions-in-python
    def excepthook_override(exctype, value, tb):
        logging.error('My Error Information')
        logging.error('Type:', exctype)
        logging.error('Value:', value)
        logging.error('Traceback:', tb)


    sys.excepthook = excepthook_override

    # Set the logging to write to terminal AND file
    logging.basicConfig(
        level=logging.INFO,
        filename='./almanac_insert_%s_to_%s_at_%s' % (
            sim_start.strftime('%Y%m%d'),
            sim_end.strftime('%Y%m%d'),
            global_start.strftime('%Y%m%d-%H%M'),
        ),
        filemode='w'
    )
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logging.info('Inserting almanacs into database')

    # TODO: Correct package imports & references
    logging.info('Getting connection')
    conn = get_connection()
    cursor = conn.cursor()

    fields = rCexec(cursor)

    logging.info('Generating dark almanac...')
    dark_alm = DarkAlmanac(sim_start, end_date=sim_end)
    logging.info('Done!')

    i = 1
    for field in fields:
        almanac = Almanac(field.ra, field.dec, sim_start, end_date=sim_end,
                          minimum_airmass=2.0, populate=True, resolution=15.)
        logging.info('Computed almanac %5d / %5d' % (i, len(fields), ))
        execute(cursor, field.field_id, almanac, dark_almanac=dark_alm)
        i += 1
        # Commit after every Almanac due to the expense of computing
        conn.commit()
        logging.info('Inserted almanac %5d / %5d' % (i, len(fields),))
