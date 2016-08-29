# For the given targets, compute which field(s) they reside in and store
# this information in the target_posn database table

import logging

from ..readout.readScience import execute as rScexec
from ..readout.readCentroids import execute as rCexec

from ....scripts.create import insert_many_rows

def execute(cursor, target_ids=None):
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

    Returns
    -------
    Nil. target_posn table is updated in place.
    """

    # Extract all the targets from the database
    targets = rScexec(cursor, unobserved=False, target_ids=target_ids)

    # Extract all the fields
    fields = rCexec(cursor)

    target_field_relations = []
    # Compute the relationship between the targets and the fields, and log
    # them for insertion into the target_posn database
    for field in fields:
        # Compute which targets are within the field
        avail_targets = field.available_targets(targets)

        # Append this information to the list of values to write back
        for tgt in avail_targets:
            target_field_relations.append((tgt.target_id, field.field_id))

    # Write the information back to the DB
    insert_many_rows(cursor, 'target_posn', target_field_relations)

    return


