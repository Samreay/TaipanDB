# For the given targets, compute which field(s) they reside in and store
# this information in the target_posn database table

import logging

from src.resources.v0_0_1.readout.readScience import execute as rScexec
from src.resources.v0_0_1.readout.readStandards import execute as rSexec
from src.resources.v0_0_1.readout.readGuides import execute as rGexec
from src.resources.v0_0_1.readout.readCentroids import execute as rCexec

from src.scripts.create import insert_many_rows

import multiprocessing
from functools import partial
import sys
import traceback
import copy

from src.scripts.connection import get_connection

# UPDATE 170623
# Parallelize the creation of target-field relationships
def make_target_field_relationship(field, cursor=None,
                                   do_guides=True, do_standards=True,
                                   targets=[], guides=[], standards=[]):
    target_field_relations = []
    logging.debug('Computing targets for field %d' % field.field_id)

    target_field_relations += [(tgt.idn, field.field_id) for tgt in
                               field.available_targets(targets)]

    if do_guides:
        logging.debug('Adding in guides')
        target_field_relations += [(tgt.idn, field.field_id) for tgt in
                                   field.available_targets(guides)]
    if do_standards:
        logging.debug('Adding in standards')
        target_field_relations += [(tgt.idn, field.field_id) for tgt in
                                   field.available_targets(standards)]

    # Because this is a separate multiprocessing function, we need to
    # duplicate the cursor
    with cursor.connection.cursor() as new_cursor:
        insert_many_rows(new_cursor, 'target_posn', target_field_relations,
                         columns=['target_id', 'field_id'])
        new_cursor.connection.commit()

    logging.info('Computed target relationships for field %d' % field.field_id)

    return


def execute(cursor, target_ids=None, field_ids=None,
            do_guides=True, do_standards=True, do_obs_targets=True,
            parallel_workers=8, active_only=True):
    """
    Compute which field(s) the targets reside in and store this information
    to the target_posn database.

    Parameters
    ----------
    cursor:
        psycopg2 cursor for interacting with the database
    target_ids:
        Optional list of target_ids to compute fields for. Defaults to None,
        at which point all targets will be extracted from the database and
        investigated.
    field_ids:
        Optional list of field_ids to compute against the target list.
        Defaults to None, at which point all fields in the database will be
        involved in the calculation.
    do_guides, do_standards, do_obs_targets:
        Optional Booleans, denoting whether position information for guides,
        standards and observed (i.e. is_done=True) targets should be calculated.
        All three default to True. These will only need to be switched to False
        in special applications; for the initial DB setup, they can just be
        left alone.

    Returns
    -------
    Nil. target_posn table is updated in place.
    """

    if do_obs_targets:
        unobserved = False
    else:
        unobserved = True

    # Extract all the targets from the database
    targets = rScexec(cursor, unobserved=unobserved, target_ids=target_ids)

    if do_guides:
        guides = rGexec(cursor)
    if do_standards:
        standards = rSexec(cursor)

    # Extract all the requested fields
    fields = rCexec(cursor, field_ids=field_ids, active_only=active_only)

    # target_field_relations = []
    # Compute the relationship between the targets and the fields, and log
    # them for insertion into the target_posn database
    logging.info('Computing target positions in each field')

    # partial_make_target_field_relationship = partial(
    #     make_target_field_relationship,
    #     cursor=copy.copy(cursor), do_guides=do_guides,
    #     do_standards=do_standards,
    #     targets=copy.deepcopy(targets), guides=copy.deepcopy(guides),
    #     standards=copy.deepcopy(standards))
    #
    # pool = multiprocessing.Pool(parallel_workers)
    # pool.map(partial_make_target_field_relationship, fields)
    # pool.close()
    # pool.join()

    # Old single-thread implementation

    for field in fields:
        logging.debug('Computing targets for field %d' % field.field_id)
        # # Compute which targets are within the field
        # avail_targets = field.available_targets(targets)
        #
        # # Append this information to the list of values to write back
        # for tgt in avail_targets:
        #     target_field_relations.append((tgt.idn, field.field_id))

        target_field_relations = [(tgt.idn, field.field_id) for tgt in
                                  field.available_targets(targets)]

        if do_guides:
            logging.debug('Adding in guides')
            target_field_relations += [(tgt.idn, field.field_id) for tgt in
                                       field.available_targets(guides)]
        if do_standards:
            logging.debug('Adding in standards')
            target_field_relations += [(tgt.idn, field.field_id) for tgt in
                                       field.available_targets(standards)]

        # Write the information back to the DB
        insert_many_rows(cursor, 'target_posn', target_field_relations,
                         columns=['target_id', 'field_id'])

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
        filename='./prep_target_posn.log',
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
    execute(cursor, do_guides=True, do_standards=True,
            active_only=True)
