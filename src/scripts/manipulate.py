# Insert data into existing DB rows
import logging
from utils import str_psql, generate_conditions_string


def update_rows_all(cursor, table, data, columns=None, conditions=None):
    """
    Update values in *all* already-existing table rows.

    Parameters
    ----------
    cursor:
        psycopg2 cursor for accessing the database.
    table:
        Which table to insert data into
    data:
        List of data to be entered (note only one list is required, as we are
        inserting the same value into all rows). Order of data should match
        the order of columns passed.
    columns:
        Optional list of column names to be written. Must be a list of column
        names which matches the order of the data points in each sub-list of
        data. Defaults to None, which means all columns will be written to in
        their database-defined order.
    conditions:
        A list of three-tuples defining conditions, e.g.:
        [(column, condition, value), ...]
        Column must be a table column name. Condition must be a *string* of a
        valid PSQL comparison (e.g. '=', '<=', 'LIKE' etc.). Value should be in
        the correct Python form relevant to the table column. Defaults to None,
        such that all rows will be affected.

    Returns
    -------
    Nil. Data is pushed into the table at the appropriate rows.
    """

    logging.info('Inserting data into table %s' % table)
    # Get the column names if they weren't passed
    if columns is None and cursor is not None:
        # Get the columns from the table itself
        cursor.execute("SELECT column_name"
                       " FROM information_schema.columns"
                       " WHERE table_name='%s'" % (table,))
        columns = cursor.fetchall()

    # Generate the PSQL string
    string = 'UPDATE %s SET ' % table
    string += ', '.join([' = '.join([columns[i], str_psql(data[i])])
                         for i in range(len(columns))])

    if conditions:
        conditions_string = generate_conditions_string(conditions)
        string += ' WHERE %s' % conditions_string

    logging.debug(string)

    if cursor:
        cursor.execute(string)

    return


def update_rows(cursor, table, data, columns=None):
    """
    Update values in already-existing table rows by matching against a
    reference column.

    Parameters
    ----------
    cursor:
        psycopg2 cursor for accessing the database.
    table:
        Which table to insert data into
    data:
        List of lists (or similar) of data to be enteted. Each sub-list
        corresponds to one row to be inserted into the database. The first
        data column *must* be the column to match rows against.
    columns:
        Optional list of column names to be written. Must be a list of column
        names which matches the order of the data points in each sub-list of
        data. Defaults to None, which means all columns will be written to in
        their database-defined order (and presumes that the column to be matched
        on is the first column in the table).

    Returns
    -------
    Nil. Data is pushed into the table at the appropriate rows.
    """

    logging.info('Inserting data into table %s' % table)
    # Get the column names if they weren't passed
    if columns is None and cursor is not None:
        # Get the columns from the table itself
        cursor.execute("SELECT column_name"
                       " FROM information_schema.columns"
                       " WHERE table_name='%s'" % (table,))
        columns = cursor.fetchall()

    # Reformat the data array into something that can be sent to psycopg
    # Note that the first item isn't written to the database (it's the
    # matching reference)
    values_string = ", ".join([str(tuple(row)) for row in data])
    values_string = "( values %s )" % values_string

    string = "UPDATE %s AS t SET %s " % (table,
                                         ','.join(["%s=c.%s" % (x, x)
                                                   for x in columns[1:]]))
    string += "FROM %s " % values_string
    string += "AS c(%s) " % ','.join(columns)
    string += "WHERE c.%s = t.%s" % (columns[0], columns[0])
    # logging.debug(string)

    if cursor is not None:
        cursor.execute(string)
        logging.info('Inserted %d rows of %s into table %s'
                     % (len(data), ', '.join(columns), table, ))
    else:
        logging.info('No database update (no cursor), but parsing %d rows '
                     'of %s into table %s parsed successfully'
                     % (len(data), ', '.join(columns), table,))

    return