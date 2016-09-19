# Insert data into existing DB rows
import logging
from utils import str_psql, str_special, str_dts, generate_conditions_string


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


def update_rows(cursor, table, data, columns=None,
                columns_to_match=1):
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
        data column(s) *must* be the column(s) to match rows against.
    columns:
        Optional list of column names to be written. Must be a list of column
        names which matches the order of the data points in each sub-list of
        data. Defaults to None, which means all columns will be written to in
        their database-defined order (and presumes that the column to be matched
        on is the first column in the table).
    columns_to_match:
        Optional; integer denoting the number of columns to match against. This
        defaults to one, such that only the first column is matched against.
        Increasing this value will cause a multiple-column match to be used.
        The columns variable *must* be defined if you wish to use
        columns_to_match.

    Returns
    -------
    Nil. Data is pushed into the table at the appropriate rows.
    """

    logging.info('Inserting data into table %s' % table)
    # Make sure that columns_to_match is valid, and matches with the number
    # of columns requested
    columns_to_match = int(columns_to_match)
    if columns_to_match <= 0:
        raise ValueError('columns_to_match must be >= 1')
    if columns_to_match > 1:
        if columns is None:
            raise ValueError('columns must be defined if using a '
                             'columns_to_match > 1')
        if len(columns) <= columns_to_match:
            raise ValueError('columns_to_match must be < the length of the '
                             'list of columns')
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
    values_string = ", ".join([str(tuple(
        [str_special(_) for _ in row]
        # row
    )) for row in data])
    values_string = "( values %s )" % values_string
    values_string = str_dts(values_string)
    logging.debug(values_string)

    string = "UPDATE %s AS t SET %s " % (table,
                                         ','.join(["%s=c.%s" % (x, x)
                                                   for x in columns[
                                                            columns_to_match:
                                                            ]]))
    string += "FROM %s " % values_string
    string += "AS c(%s) " % ','.join(columns)
    string += " AND ".join(["WHERE c.%s = t.%s" % (columns[i], columns[i])
                            for i in range(columns_to_match)])
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


def increment_rows(cursor, table, column, ref_column=None, ref_values=None,
                   inc=1):
    """
    Increment the values of a particular column for a particular set of rows.
    Parameters
    ----------
    cursor:
        psycopg2 cursor for communicating with the database
    table:
        The database table holding the column needing incrementing
    column:
        The column to be incremented.
    ref_column:
        Optional; the reference column for checking which rows to increment.
        Is paired with ref_values, which must be passed if ref_column is.
        Defaults to None, such that all rows will have 'column' incremented.
    ref_values:
        Optional; a single value of list of values for checking which rows
        to increment. Rows will only be incremented if the row's ref_column
        value is in ref_values. Must be passed with ref_column. Defaults to
        None, such that all rows will have 'column' incremented.
    inc:
        Optional; integer, specifying the amount to increment. Defaults to 1.

    Returns
    -------
    Nil. Database is updated in-situ.
    """

    logging.info('Incrementing data into table %s' % table)
    # Get the column names
    if cursor is not None:
        # Get the columns from the table itself
        cursor.execute("SELECT column_name"
                       " FROM information_schema.columns"
                       " WHERE table_name='%s'" % (table,))
        columns = cursor.fetchall()
        # columns = list([c[0] for c in columns])
        logging.debug(columns)
    else:
        # Going to do input checking, need to return now
        logging.warning('Cursor is None, not performing database actions')
        return

    # Input checking
    if (ref_column is None and ref_values is not None) or \
            (ref_column is not None and ref_values is None):
        raise ValueError('ref_column and ref_values must both be passed or '
                         'both left as None')
    # if column not in columns:
    #     raise ValueError('column %s not found in %s' % (column, table, ))
    # if ref_column is not None and ref_column not in columns:
    #     raise ValueError('reference column %s not found in %s' %
    #                      (column, table, ))
    if ref_values is not None:
        # Check if the list has zero-length; if so, we can abort now
        if len(ref_values) == 0:
            logging.debug('No rows need incrementing - exiting increment_rows')
            return
        # List-ize the ref_values in case a lone value was passed
        ref_values = list(ref_values)

    # Construct the database query string
    string = 'UPDATE %s ' % (table, )
    string += 'SET %s = %s + %d ' % (column, column, inc)
    if ref_column is not None:
        conditions_string = generate_conditions_string([
            (ref_column, 'IN', tuple(ref_values)),
        ])
        string += 'WHERE ' + conditions_string

    logging.debug(string)
    cursor.execute(string)
    return
