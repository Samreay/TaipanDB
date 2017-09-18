# This is a utility script, designed to reset the database to its
# pre-simulation state
# Note that if database structures have changed, or *any* target/field
# catalogues have been updated, a standard database rebuild will
# be required

import logging

from taipan.simulate.utils.updatesci import update_science_targets


def execute(cursor):
    """
    .. warning:: DEPRECATED - DO NOT USE
    """
    # Remove the tiles
    logging.info('Deleting tiles...')
    cursor.execute('DELETE FROM tile')
    logging.info('Done!')
    # Reset the target visits/repeats/observations, and undo any
    # done/success information
    logging.info('Resetting target information...')
    cursor.execute('UPDATE science_target SET '
                   'observations=0,'
                   'visits=0,'
                   'repeats=0,'
                   'done=NULL,'
                   'success=False')
    cursor.execute('UPDATE science_target SET '
                   'success=true WHERE '
                   'has_sdss_zspec')
    logging.info('Done!')
    # Re-compute the target priority and difficulty information
    logging.info('Updating science target information...')
    update_science_targets(cursor, do_tp=True, do_d=True)
    logging.info('Done!')

    cursor.connection.commit()

    return
