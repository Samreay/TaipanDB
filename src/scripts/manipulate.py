# Insert data into existing DB rows
import logging
from utils import str_psql, str_special, str_dts, generate_conditions_string
from create import insert_many_rows, create_index
from extract import extract_from
import numpy as np


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
                columns_to_match=1,
                conditions=None):
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

    if conditions:
        conditions_string = generate_conditions_string(conditions)
        # Note we use AND here, because WHERE has already been invoked above
        string += ' AND %s' % conditions_string

    logging.debug(string)

    if cursor is not None:
        cursor.execute(string)
        logging.info('Inserted %d rows of %s into table %s'
                     % (len(data), ', '.join(columns), table, ))
    else:
        logging.info('No database update (no cursor), but parsing %d rows '
                     'of %s into table %s parsed successfully'
                     % (len(data), ', '.join(columns), table,))

    return


def update_rows_temptable(cursor, table, data, columns=None,
                          columns_to_match=1,
                          conditions=None):
    """
    As for update_rows, but does it using a temporary table rather than
    storing the information directly in the query - more performant for
    large (thousand+ row) inserts.

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

    logging.info('Inserting data into table %s using a temp table' % table)
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
    if cursor is not None:
        # Get the columns from the table itself if not provided,
        # otherwise get data types for requested columns
        cursor.execute("SELECT column_name,data_type"
                       " FROM information_schema.columns"
                       " WHERE table_name='%s'" % (table,))
        db_columns = cursor.fetchall()
        if columns is None:
            raise ValueError('Although a keyword argument, '
                             'update_rows_temptable cannot be called without '
                             'specifying columns')
        else:
            db_columns = [_ for _ in db_columns if _[0] in columns]

    # Create a temporary table to hold the data we wish to update
    logging.debug('Creating temp table')
    cursor.execute("CREATE TEMPORARY TABLE update_rows_temp "
                   "(%s) " % ', '.join(['%s %s' % _ for
                                        _ in db_columns if
                                        _[0] in columns])
                   )
    # Insert the data into the temporary table
    logging.debug('Writing to temp table')
    insert_many_rows(cursor, 'update_rows_temp', data, columns=columns)
    # Create an index on the columns_to_match to increase speed
    create_index(cursor, 'update_rows_temp',
                 columns[:columns_to_match])

    # Copy the data from the temporary table to the 'live' table
    logging.debug('Copying data to live table')
    cursor.execute('UPDATE %s '
                   'SET %s '
                   'FROM update_rows_temp x '
                   'WHERE %s' %
                   (table,
                    ','.join(['%s=x.%s' % (c, c, ) for c in
                              columns[columns_to_match:]]),
                    ','.join(['%s.%s=x.%s' % (table, c, c, ) for c in
                              columns[:columns_to_match]]), )
                   )

    # Kill the temporary table
    logging.debug('DROPing temporary table')
    cursor.execute('DROP TABLE update_rows_temp')

    logging.debug('update_rows_temptable done!')

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


def upsert_many_rows(cursor, table, data, columns=None):
    """
    'Upsert' many rows into the database. An 'upsert' is where a row is
    attempted to be inserted, but if the row already exists (i.e. the
    primary key value(s) already exist), the existing row is updated instead.

    Due to the restrictions on the new ON CONFLICT functionality of the
    INSERT command (PostGres 9.5), it is only possible to match rows on their
    *full* primary key (that is, the monolithic primary key, or the combination
    of values that forms the primary key).

    For ease of forming the database query, we *require* that the primary key
    column(s) are the first columns in values/columns. This is checked, and an
    error will be thrown if you don't do this.

    Parameters
    ----------
    cursor:
        psycopg2 cursor for interacting with the database.
    table:
        Database table to insert values to.
    data:
        The values to be added to the table, as a list. Values
        should be in the order of the database columns, unless the columns
        argument is also passed; in that case, values should be in order
        corresponding to the columns parameter.
    columns:
        List of column names that correspond to the ordering of values. Can also
        be used to restrict the number of columns to write to (i.e. allow
        default table values for columns if not required). Defaults to None,
        which assumes that you wish to write information to all columns, and
        that the columns are ordered in the way defined in the database.

    Returns
    -------
    Nil. Data are inserted/updated in the database.
    """
    # Get the primary key columns for this table
    pkey_query = "SELECT a.attname, format_type(a.atttypid, a.atttypmod) " \
                 "AS data_type FROM   pg_index i " \
                 "JOIN   pg_attribute a ON a.attrelid = i.indrelid " \
                 "AND a.attnum = ANY(i.indkey) " \
                 "WHERE  i.indrelid = '%s'::regclass " \
                 "AND    i.indisprimary;" % (table, )
    cursor.execute(pkey_query)
    pk_names = [_[0] for _ in cursor.fetchall()]

    # Get the column names if they weren't passed
    if columns is None and cursor is not None:
        # Get the columns from the table itself
        cursor.execute("SELECT column_name"
                       " FROM information_schema.columns"
                       " WHERE table_name='%s'" % (table,))
        columns = [_[0] for _ in cursor.fetchall()]

    # Check that the primary key columns are the first columns listed - if
    # not, raise an error
    pk_is = [columns.index(p) for p in pk_names]
    if np.max(pk_is) > len(pk_is):
        raise ValueError("The primary key columns must be the first columns "
                         "you specify in the columns option. If you didn't "
                         "pass a columns option, something is wrong with the "
                         "database construction.")

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

    # Generate the string to insert the values
    query_string = "INSERT INTO %s %s %s" % (
        table,
        "" if columns is None else "(" + ", ".join(columns) + ")",
        values_string,
    )
    query_string += " ON CONFLICT (%s) DO UPDATE SET " % (','.join(pk_names), )
    query_string += " AND ".join(["%s = EXCLUDED.%s" % (c, c) for
                                  c in columns[len(pk_names):]])

    cursor.execute(query_string)

    return
