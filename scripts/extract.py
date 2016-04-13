import logging
import numpy as np
import re
import psycopg2


# psql-numpy data type relationship
PSQL_TO_NUMPY_DTYPE = {
    "smallint": "int16",
    "integer": "int32",
    "bigint": "int64",
    "decimal": "float64",
    "numeric": "float64",
    "real": "float32",
    "double precision": "float64",
    "smallserial": "int16",
    "serial": "int32",
    "bigserial": "int64",
    "boolean": "bool",
    "text": "str",
}


# Helper function - this should be called rather than the dict unless
# you're SURE that you won't come across a char(n) or varchar(n)
def psql_to_numpy_dtype(psql_dtype):
    """
    Converts a PSQL data type to the corresponding numpy data type. Used for
    converting the return of a psycopg2 query to a numpy structured array.

    Parameters
    ----------
    psql_dtype:
        The PSQL data type to be converted. Note that, for all data types except
        char(n) and varchar(n), the function will simply perform a lookup in the
        dictionary PSQL_TO_NUMPY_DTYPE.

    Returns
    -------
    numpy_dtype:
        The corresponding numpy data type.
    """

    # Handle char, varchar
    if 'char(' in psql_dtype:
        regex = re.compile(r'^[a-z]*char\((?P<len>[0-9]*)\)$')
        match = regex.search(psql_dtype)
        if not match:
            raise ValueError('Invalid char data type passed to'
                             ' psql_to_numpy_dtype')
        return 'S%s' % (match.group('len'))

    # All other types
    return PSQL_TO_NUMPY_DTYPE[psql_dtype]


def extract_from(cursor, table, conditions=None, columns=None):
    """
    Extract rows from a database table.

    Parameters
    ----------
    cursor:
        The psycopg2 cursor that interacts with the relevant database.
    table:
        The name of the table to be read.
    conditions:
        List of tuples denoting conditions to be supplied, in the form
        [(column1, condition1), (column2, condition2), ...]
        All conditions are assumed to be equalities. Defaults to None.
    columns:
        List of column names to retrieve from the database. Defaults to None,
        which returns all available columns.

    Returns
    -------
    result:
        A numpy structured array of all table rows which satisfy conditions (if
        given). Individual entry elements may be called by column name.
    """

    if cursor is not None:
        # Get the column names from the table itself
        cursor.execute("SELECT column_name, data_type"
                       " FROM information_schema.columns"
                       " WHERE table_name='%s'" % (table, ))
        table_structure = cursor.fetchall()
        logging.debug(table_structure)
        table_columns, dtypes = zip(*table_structure)
        if columns is None:
            columns = table_columns
        else:
            columns_lower = [x.lower() for x in columns]
            dtypes = [dtypes[i] for i in range(len(dtypes))
                      if table_columns[i].lower()
                      in columns_lower]

    string = "SELECT %s FROM %s" % (
        "*" if columns is None else ", ".join(columns),
        table,
        )

    if conditions:
        conditions_string = ' WHERE ' + ' AND '.join([' = '.join(map(str, x))
                                                      for x in conditions])
        string += conditions_string

    logging.debug(string)

    if cursor is not None:
        cursor.execute(string)
        result = cursor.fetchall()
        logging.debug("Extract successful")
    else:
        result = None
        return result

    # Re-format the result as a structured numpy table
    logging.debug('Attempting to make numpy structured array from:')
    logging.debug(len(result))
    logging.debug(columns)
    logging.debug(dtypes)
    result = np.asarray(result, dtype={
        "names": columns,
        "formats": [psql_to_numpy_dtype(dtype) for dtype in dtypes],
        })

    return result


def extract_from_joined(cursor, tables, conditions=None, columns=None):
    """
    Extract rows from a database table join.

    Parameters
    ----------
    cursor:
        The psycopg2 cursor that interacts with the relevant database.
    tables:
        List of table names to be joined. Tables are joined using NATURAL JOIN,
        which requires that the join-ing column have the same name in each
        table. If this is not possible, some other solution will need to be
        implemented.
    conditions:
        List of tuples denoting conditions to be supplied, in the form
        [(column1, condition1), (column2, condition2), ...]
        All conditions are assumed to be equalities. Defaults to None.
    columns:
        List of column names to retrieve from the database. Defaults to None,
        which returns all available columns.

    Returns
    -------
    result:
        A numpy structured array of all joined table rows which satisfy
        conditions (if given). Individual entry elements may be called by
        column name.
    """

    if cursor is not None:
        # Get the column names from the table itself
        table_string = "SELECT column_name, data_type FROM" \
                       " information_schema.columns " \
                       "WHERE table_name IN (%s)" %\
                       (','.join(map(lambda x: "'%s'" % x, tables)), )
        logging.debug(table_string)
        cursor.execute(table_string)
        table_structure = cursor.fetchall()
        try:
            table_columns, dtypes = zip(*table_structure)
        except ValueError:
            # This occurs when one of the tables in empty
            logging.info('At least one of the requested tables has no columns')
            return []
        if columns is None:
            columns = table_columns
        else:
            columns_lower = [x.lower() for x in columns]
            dtypes = [dtypes[i] for i in range(len(dtypes))
                      if table_columns[i].lower()
                      in columns_lower]
        logging.debug('Found these columns with these data types:')
        logging.debug(columns)
        logging.debug(dtypes)
    string = 'SELECT %s FROM %s' % (
        "*" if columns is None else ", ".join(columns),
        ' NATURAL JOIN '.join(tables),
        )

    if conditions:
        conditions_string = ' WHERE '.join([' = '.join(map(str, x))
                                            for x in conditions])
        string += conditions_string

    logging.debug(string)

    if cursor is not None:
        cursor.execute(string)
        result = cursor.fetchall()
        logging.debug("Extract successful")
    else:
        result = None
        return result

    # Re-format the result as a structured numpy table
    result = np.asarray(result, dtype={
        "names": columns,
        "formats": [psql_to_numpy_dtype(dtype) for dtype in dtypes],
        })

    return result


def execute_select(connection, statement):
    """ Execute an arbitrary SELECT statement.

    Uses a new cursor.

    Parameters
    ----------
    connection : psycopg2 connection
        The database connection with which to generate a cursor from
    statement : str
        The SELECT statement query to execute

    Returns
    -------
        list
            The results of the query, each row being an element in the list.
    """
    cursor = connection.cursor()
    assert statement.upper().find(
        "SELECT"
    ) == 0, "You must submit a SELECT statement, that begins with SELECT"
    try:
        logging.info("Executing statement: %s" % statement)
        cursor.execute(statement)
    except psycopg2.ProgrammingError as e:
        logging.error(e)
        return []
    result = cursor.fetchall()
    logging.info("Found %d rows" % len(result))
    return result


