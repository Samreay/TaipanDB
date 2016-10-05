# Insert calculated almanacs into the database

import logging
# from taipan.core import TaipanTile
from taipan.scheduling import DarkAlmanac, ephem_to_dt, localize_utc_dt
from ....scripts.create import insert_many_rows
from ....scripts.manipulate import upsert_many_rows
# from ....scripts.extract import extract_from, extract_from_joined
import numpy as np

import datetime

from ..readout import readScience as rSc


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


def execute(cursor, field_id, almanac, dark_almanac=None, update=False):
    """
    Insert the given tiles into the database.

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
                           map(lambda x: localize_utc_dt(ephem_to_dt(x)),
                               almanac.data['date']),
                           almanac.data['airmass'],
                           dark_almanac.data['sun_alt'],
                           dark_almanac.data['dark_time']]).T
    logging.debug(data_out)
    logging.debug(data_out.shape)

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
