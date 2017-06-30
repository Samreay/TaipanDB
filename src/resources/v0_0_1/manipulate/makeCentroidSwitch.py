# Switch status of all fields from active to inactive, or vice versa

from ..readout import readCentroids as rC
from ....scripts.manipulate import update_rows_all


def execute(cursor):
    """
    Flip the status of all centroids (i.e. fields) in the database from
    active to inactive, or vice versa.

    This function assumes that we will only ever conduct a direct switch
    of fields from active to inactive. More complex behaviour will need to
    be handled with a manually-constructed script, or a more complex function
    to be developed later.

    Parameters
    ----------
    cursor : psycopg2.connection.cursor object
        For communication with the database

    Returns
    -------
    Nil. Database updated in-situ.
    """

    # Get all fields, and those fields which are currently active
    all_fields = rC.execute(cursor, active_only=False)
    all_fields = [t.field_id for t in all_fields]
    active_fields = rC.execute(cursor, active_only=True)
    active_fields = [t.field_id for t in active_fields]

    # Make the switches
    update_rows_all(cursor, 'field', [False, ], columns=['is_active', ],
                    conditions=[('field_id', 'IN', active_fields), ])
    update_rows_all(cursor, 'field', [True, ], columns=['is_active', ],
                    conditions=[('field_id', 'IN',
                                 [f for f in all_fields if
                                  f not in active_fields]), ])

    return
