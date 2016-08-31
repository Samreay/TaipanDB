import logging
import psycopg2
import numpy as np
from .utils import generate_conditions_string


def delete_rows(cursor, table, conditions=None):
    """
    Drop specified rows from a database table.

    Parameters
    ----------
    cursor:
        psycopg2 cursor for interacting with the database
    table:
        Database table to drop rows from.
    conditions:
        conditions:
        A list of three-tuples defining conditions, e.g.:
        [(column, condition, value), ...]
        Column must be a table column name. Condition must be a *string* of a
        valid PSQL comparison (e.g. '=', '<=', 'LIKE' etc.). Value should be in
        the correct Python form relevant to the table column. Defaults to None,
        so all rows will be deleted.

    Returns
    -------
    Nil. Matching rows are deleted from the relevant table. Note that CASCADING
    deletes are implemented, so any rows in other tables linked by a foreign
    key to the deleted rows will also be deleted.
    """
    if cursor is not None:
        # Get the column names from the table itself
        cursor.execute("SELECT column_name, data_type"
                       " FROM information_schema.columns"
                       " WHERE table_name='%s'" % (table, ))
        table_structure = cursor.fetchall()
        logging.debug(table_structure)
        columns, dtypes = zip(*table_structure)
        columns_lower = [x.lower() for x in columns]
        dtypes = [dtypes[i] for i in range(len(dtypes))
                  if columns[i].lower()
                  in columns_lower]

    string = 'DELETE * FROM %s' % table

    if conditions:
        conditions_string = generate_conditions_string(conditions)
        string += ' WHERE %s' % conditions_string

    logging.debug(string)

    if cursor is not None:
        cursor.execute(string)

    return