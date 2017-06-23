# For the given targets, compute which field(s) they reside in and store
# this information in the target_posn database table

import logging

from ..readout.readScience import execute as rScexec
from ..readout.readStandards import execute as rSexec
from ..readout.readGuides import execute as rGexec

from ..readout.readCentroids import execute as rCexec

from ....scripts.create import insert_many_rows

import multiprocessing
from functools import partial


def execute(cursor, target_ids=None, field_ids=None,
            do_guides=True, do_standards=True, do_obs_targets=True,
            parallel_workers=8):
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
    fields = rCexec(cursor, field_ids=field_ids)

    target_field_relations = []
    # Compute the relationship between the targets and the fields, and log
    # them for insertion into the target_posn database
    logging.info('Computing target positions in each field')

    # UPDATE 170623
    # Parallelize the creation of target-field relationships
    def make_target_field_relationship(field, cursor=cursor):
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
        new_cursor = cursor.connection.cursor()
        insert_many_rows(new_cursor, 'target_posn', target_field_relations,
                     columns=['target_id', 'field_id'])
        new_cursor.connection.commit()

        return
    partial_make_target_field_relationship = partial(
        make_target_field_relationship, cursor=cursor)

    pool = multiprocessing.Pool(parallel_workers)
    pool.map(partial_make_target_field_relationship, fields)
    pool.close()
    pool.join()

    return

    # Old single-thread implementation

    # for field in fields:
    #     logging.debug('Computing targets for field %d' % field.field_id)
    #     # # Compute which targets are within the field
    #     # avail_targets = field.available_targets(targets)
    #     #
    #     # # Append this information to the list of values to write back
    #     # for tgt in avail_targets:
    #     #     target_field_relations.append((tgt.idn, field.field_id))
    #
    #     target_field_relations += [(tgt.idn, field.field_id) for tgt in
    #                                field.available_targets(targets)]
    #
    #     if do_guides:
    #         logging.debug('Adding in guides')
    #         target_field_relations += [(tgt.idn, field.field_id) for tgt in
    #                                    field.available_targets(guides)]
    #     if do_standards:
    #         logging.debug('Adding in standards')
    #         target_field_relations += [(tgt.idn, field.field_id) for tgt in
    #                                    field.available_targets(standards)]
    #
    # # Write the information back to the DB
    # insert_many_rows(cursor, 'target_posn', target_field_relations,
    #                  columns=['target_id', 'field_id'])

    return


