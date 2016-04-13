# Insert data into existing DB rows
import logging

def insert(cursor, table, data, columns=None):
    """
    Insert rows into an existing table.

    Parameters
    ----------
    cursor:
        psycopg2 cursor for accessing the database.
    table:
        Name of the table to be written to.
    data:
        List of lists (or similar) of data to be enteted. Each sub-list
        corresponds to one row to be inserted into the database.
    columns:
        Optional list of column names to be written. Must be a list of column
        names which matches the order of the data points in each sub-list of
        data. Defaults to None, which means all columns will be written to in
        their database-defined order.

    Returns
    -------
    Nil. Rows are added to the database.
    """
    pass


def update(cursor, table, data, columns=None):
    """
    Update values in already-existing table rows.

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

    # Get the column names if they weren't passed
    if columns is None and cursor is not None:
        # Get the columns from the table itself
        cursor.execute("SELECT column_name"
                       " FROM information_schema.columns"
                       " WHERE table_name='%s'" % (table,))

    # Reformat the data array into something that can be sent to psycopg
    values_string = ",".join([str(tuple(row)) for row in data])
    values_string = "( values %s )" % values_string

    string = "UPDATE %s AS t SET %s " % (table,
                                         ','.join(["%s=c%s" % x
                                                   for x in columns]))
    string += "FROM %s " % values_string
    string += "AS c(%s) " % ','.join(columns)
    string += " WHERE c.%s = t.%s" % (columns[0], columns[0])
    logging.debug(string)

    return
