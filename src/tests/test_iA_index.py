# Work out if it's more efficient to query the DB with many small queries,
# or a few big queries

import logging
import datetime
import sys
import random
import pickle
import traceback

from src.scripts.connection import get_connection
from src.resources.v0_0_1.readout import readTiles as rT
from src.resources.v0_0_1.insert import insertTiles as iT

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
        filename='./testlog_iA_index.log',
        filemode='w'
    )
    logger = logging.getLogger()
    logger.setLevel(logging.WARNING)
    console = logging.StreamHandler()
    console.setLevel(logging.WARNING)
    logging.warning('TESTING TILE INSERTION WITH INDEX REMOVAL/REGEN.')

    # Get a cursor
    # TODO: Correct package imports & references
    logging.debug('Getting connection')
    conn = get_connection()
    cursor = conn.cursor()

    # Grab some tiles to work on
    logging.warning('Fetching tiles...')
    try:
        with open('tiles.pobj', 'r') as fileobj:
            tile_list = pickle.load(fileobj)
    except IOError:
        tile_list = rT.execute(cursor,)
    logging.warning('...done!')

    logging.warning('Starting test...')
    for n in [1, 10, 100, 200]:
        start = datetime.datetime.now()
        iT.execute(cursor, tile_list[:n], remove_index=False)
        end = datetime.datetime.now()
        delta = (end - start).total_seconds()
        logging.warning('   Inserted %04d tiles in %5.1fs' % (n, delta, ))

        start = datetime.datetime.now()
        iT.execute(cursor, tile_list[:n], remove_index=True)
        end = datetime.datetime.now()
        delta = (end - start).total_seconds()
        logging.warning('   Inserted %04d tiles in %5.1fs with index removal' %
                        (n, delta,))

    logging.warning('...test complete!')
