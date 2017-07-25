import logging
import pickle
import datetime
import sys

from src.resources.v0_0_1.readout import readCentroids as rC
from src.scripts.connection import get_connection

from taipan.scheduling import Almanac, DarkAlmanac

import multiprocessing
from functools import partial

ALMANAC_FILE_LOC = '/data/resources/0.0.1/'


def make_almanac_n(field, sim_start=datetime.datetime.now(),
                   sim_end=datetime.datetime.now() + datetime.timedelta(1),
                   minimum_airmass=2.0,
                   resolution=15.):
    almanac = Almanac(field.ra, field.dec, sim_start, end_date=sim_end,
                      minimum_airmass=minimum_airmass, populate=True,
                      resolution=resolution)
    logging.info('Computed almanac for field %5d' % (field.field_id, ))
    with open(ALMANAC_FILE_LOC + 'alm_%5d.pobj' % field.field_id, 'w') as \
            fileobj:
        pickle.dump(almanac, fileobj)
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
        level=logging.WARNING,
        filename='./loadlog_rASF.log',
        filemode='w'
    )
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    console = logging.StreamHandler()

    sim_start = datetime.date(2017, 4, 1)
    sim_end = datetime.date(2024, 1, 1)

    logging.info('Generating dark almanac...')
    dark_alm = DarkAlmanac(sim_start, end_date=sim_end)
    with open(ALMANAC_FILE_LOC + 'alm_dark.pobj', 'w') as fileobj:
        pickle.dump(dark_alm, fileobj)
    logging.info('Done!')

    cursor = get_connection().cursor()

    logging.info('Generating standard almanacs...')
    create_almanac_files(cursor, sim_start, sim_end, resolution=15.,
                         minimum_airmass=2.0, active_only=False)
    logging.info('...done!')