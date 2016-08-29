# For the given targets, compute which field(s) they reside in and store
# this information in the target_posn database table

import logging

from ..readout.readScience import execute as rScexec
from ..readout.readCentroids import execute as rCexec

from ....scripts.create import insert_many_rows


def execute(cursor, target_ids=None, field_ids=None):
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

    Returns
    -------
    Nil. target_posn table is updated in place.
    """

    # Extract all the targets from the database
    targets = rScexec(cursor, unobserved=False, target_ids=target_ids)

    # Extract all the requested fields
    fields = rCexec(cursor, field_ids=field_ids)

    target_field_relations = []
    # Compute the relationship between the targets and the fields, and log
    # them for insertion into the target_posn database
    logging.info('Computing target positions in each field')
    for field in fields:
        logging.debug('Computing targets for field %d' % field.field_id)
        # Compute which targets are within the field
        avail_targets = field.available_targets(targets)1

        # Append this information to the list of values to write back
        for tgt in avail_targets:
            target_field_relations.append((tgt.idn, field.field_id))

    # Write the information back to the DB
    insert_many_rows(cursor, 'target_posn', target_field_relations,
                     columns=['target_id', 'field_id'])

    return


