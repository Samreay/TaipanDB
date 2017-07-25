import logging
import pickle
import datetime

from src.resources.v0_0_1.readout import readCentroids as rC

from taipan.scheduling import Almanac

import multiprocessing
from functools import partial

ALMANAC_FILE_LOC = '/data/src/resources/v0_0_1/static/'


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
