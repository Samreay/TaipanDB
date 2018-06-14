# Output the observing logs to pickle files

import datetime
import pickle

from taipandb.resources.v0_0_1.readout import readObservingLog as rOL
from taipandb.resources.v0_0_1.readout import readTileObservingInfo as rTOI

from taipandb.scripts.connection import get_connection


def execute(cursor, dt=datetime.datetime.now(), flag='progress',
            output_path='.'):
    """
    Output a Python pickle object containing the observing log information
    
    Parameters
    ----------
    cursor : :obj:`psycopg2.connection.cursor`
        psycopg2 cursor for database connections
    dt : :obj:`datetime.datetime`, optional
        Datetime at which to mark the output file. Defaults to 
        datetime.datetime.now().
    flag : :obj:`str`, optional
        Flag to add to the end of the filename (e.g. 'progress', 'abort', 
        'final', etc.). Defaults to 'progress'. Set to None to not have a flag.
    output_path: :obj:`str`, optional
        Relative or absolute path to place the file in. Defaults to '.' (i.e.
        the present working directory).

    Returns
    -------
    file_path : :obj:`str`
        The file path to the output file. Will be relative if output_path was
        defined in a relative fashion.
    """

    obs_log = rOL.execute(cursor)
    tile_obs_log = rTOI.execute(cursor)
    outputs = [obs_log, tile_obs_log, ]

    with open('%s/results-%s%s.pobj' %
              (output_path,
               dt.strftime('%y%m%d-%H%M'),
               '-%s' % flag if flag is not None else ''), 'w') as fileobj:
        pickle.dump(outputs, fileobj)
        return fileobj.name


if __name__ == '__main__':
    # Grab a cursor
    cursor = get_connection().cursor()

    # Run the function
    execute(cursor, flag='cmdline')
