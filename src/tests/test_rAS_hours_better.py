# Work out if it's more efficient to query the DB with many small queries,
# or a few big queries

import logging

import sys
import multiprocessing
from functools import partial
import traceback
import datetime
import random
import numpy as np

from src.resources.v0_0_1.readout import readAlmanacStats as rAS
from src.resources.v0_0_1.readout import readCentroids as rC

from src.scripts.connection import get_connection


def rAS_hours_obs_single(field_id,
                         datetime_from=datetime.datetime(2018,1,1,0,0),
                         datetime_to=datetime.datetime(2018,1,1,12,0),
                         **kwargs):
    with get_connection().cursor() as cursor:
        return rAS.hours_observable(cursor, field_id,
                                    datetime_to, datetime_from,
                                    **kwargs)


def rAS_hours_obs_bulk(field_ids,
                       datetime_from=datetime.datetime(2018,1,1,0,0),
                       datetime_to=datetime.datetime(2018,1,1,12,0),
                       **kwargs):
    with get_connection().cursor() as cursor:
        return rAS.hours_observable_bulk(cursor, field_ids,
                                         datetime_to, datetime_from,
                                         **kwargs)

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
        filename='./testlog_rAS_hours_better.log',
        filemode='w'
    )
    logger = logging.getLogger()
    logger.setLevel(logging.WARNING)
    console = logging.StreamHandler()
    console.setLevel(logging.WARNING)
    logging.warning('TESTING PARALLEL SCORING')
    logging.warning('NOTE: Your system has %d cores' %
                    multiprocessing.cpu_count())

    # Get a cursor
    # TODO: Correct package imports & references
    logging.debug('Getting connection')
    conn = get_connection()
    cursor = conn.cursor()

    results_dict = {
        'mono-multi': [],
        'mono-single': [],
        'batch-single': [],
        'batch-multi': [],
    }

    dt_f = datetime.datetime(2017, 10, 1, 0, 0)
    dt_t = datetime.datetime(2019, 10, 1, 0, 0)

    for i in range(5):
        logging.warning('Pass %d...' % (i+1, ))

        cents = rC.execute(cursor)
        fields = [_.field_id for _ in random.sample(cents, 3200)]

        # Batch-single
        logging.warning('   Batch, single...')
        start = datetime.datetime.now()
        _ = [rAS.hours_observable_bulk(cursor, fields[i:i + 800], dt_f, dt_t)
             for
             i in range(0, 3200, 800)]
        end = datetime.datetime.now()
        delta = end - start
        results_dict['batch-single'].append(delta.total_seconds())

        # Batch-parallel
        logging.warning('   Batch, parallel...')
        start = datetime.datetime.now()
        rAS_hrs_obs_bulk_partial = partial(rAS_hours_obs_bulk,
                                           datetime_from=dt_f, datetime_to=dt_t,
                                           )
        pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
        _ = pool.map(rAS_hrs_obs_bulk_partial,
                     [fields[i:i + 800] for i in range(0, 3200, 800)])
        pool.close()
        pool.join()
        end = datetime.datetime.now()
        delta = end - start
        results_dict['batch-multi'].append(delta.total_seconds())

        logging.warning('...done!')

        # Mono-single
        logging.warning('   Mono, single...')
        start = datetime.datetime.now()
        _ = [rAS.hours_observable(cursor, f, dt_f, dt_t) for f in fields]
        end = datetime.datetime.now()
        delta = end - start
        results_dict['mono-single'].append(delta.total_seconds())

        # Mono-parallel
        logging.warning('   Mono, parallel...')
        start = datetime.datetime.now()
        rAS_hrs_obs_partial = partial(rAS_hours_obs_single, datetime_from=dt_f,
                                      datetime_to=dt_t, )
        pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
        _ = pool.map(rAS_hrs_obs_partial, fields)
        pool.close()
        pool.join()
        end = datetime.datetime.now()
        delta = end - start
        results_dict['mono-multi'].append(delta.total_seconds())


    logging.warning('RESULTS (%d passes)' % 5)
    logging.warning('-------')
    for k, v in results_dict.items():
        logging.warning('%s: %.1f +/- %.1f s' % (
            k, np.average(v), np.std(v),
        ))
    logging.warning('-------')