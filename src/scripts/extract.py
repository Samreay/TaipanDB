import logging
import numpy as np
import re
import psycopg2
from .utils import generate_conditions_string, \
    generate_case_conditions_string
import datetime


# psql-numpy data type relationship
PSQL_TO_NUMPY_DTYPE = {
    "smallint": "int64",
    "integer": "int64",
    "bigint": "int64",
    "decimal": "float64",
    "numeric": "float64",
    "real": "float64",
    "double precision": "float64",
    "smallserial": "int64",
    "serial": "int64",
    "bigserial": "int64",
    "boolean": "bool",
    "text": "str",
    "timestamp": datetime.datetime,
    "timestamp without time zone": datetime.datetime,
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


def get_columns(cursor, table):
    """
    Return the columns names and data types for a particular table.
    Parameters
    ----------
    cursor:
        psycopg2 cursor to interact with the database
    table:
        Name of the table to be investigated

    Returns
    -------
    table_columns:
        A list of the table columns in table
    dtypes:
        The SQL data types of table_columns.

    """
    if cursor is not None:
        # Get the column names from the table itself
        cursor.execute("SELECT column_name, data_type"
                       " FROM information_schema.columns"
                       " WHERE table_name='%s'" % (table,))
        table_structure = cursor.fetchall()
        logging.debug(table_structure)
        table_columns, dtypes = zip(*table_structure)
        return table_columns, dtypes


def extract_from(cursor, table, conditions=None, columns=None,
                 conditions_combine='AND', distinct=False):
    """
    Extract rows from a database table.

    Parameters
    ----------
    cursor:
        The psycopg2 cursor that interacts with the relevant database.
    table:
        The name of the table to be read.
    conditions:
        A list of three-tuples defining conditions, e.g.:
        [(column, condition, value), ...]
        Column must be a table column name. Condition must be a *string* of a
        valid PSQL comparison (e.g. '=', '<=', 'LIKE' etc.). Value should be in
        the correct Python form relevant to the table column. Defaults to None,
        so all rows will be returned.
    columns:
        List of column names to retrieve from the database. Defaults to None,
        which returns all available columns.
    conditions_combine:
        Optional; string determining how the conditions should be combined.
        Defaults to 'AND'.

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

    string = "SELECT %s %s FROM %s" % (
        "DISTINCT" if distinct else "",
        "*" if columns is None else ", ".join(columns),
        table,
        )

    if conditions:
        conditions_string = generate_conditions_string(conditions,
                                                       combine=
                                                       conditions_combine)
        string += ' WHERE %s' % conditions_string

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


def count_from(cursor, table, conditions=None,
               conditions_combine='AND',
               case_conditions=None,
               case_conds_combine='AND'):
    """
    Count the number of rows in a database table.

    Parameters
    ----------
    cursor:
        The psycopg2 cursor that interacts with the relevant database.
    table:
        The name of the table to be read.
    conditions:
        A list of three-tuples defining conditions, e.g.:
        [(column, condition, value), ...]
        Column must be a table column name. Condition must be a *string* of a
        valid PSQL comparison (e.g. '=', '<=', 'LIKE' etc.). Value should be in
        the correct Python form relevant to the table column. Defaults to None,
        so all rows will be returned.
    conditions_combine:
        Optional; string determining how the conditions should be combined.
        Defaults to 'AND'.

    Returns
    -------
    result:
        The number of rows in the database table, satisfying any conditions
        which may have been passed.
    """

    string = "SELECT COUNT(*) FROM %s" % (
        table,
        )

    if conditions or case_conditions:
        string += ' WHERE '

    if conditions:
        conditions_string = generate_conditions_string(conditions,
                                                       combine=
                                                       conditions_combine)
        string += conditions_string

    if cursor is not None:
        cursor.execute(string)
        result = cursor.fetchall()
        logging.debug("Extract successful")
    else:
        result = None
        return result


    return int(result[0][0])


def extract_from_joined(cursor, tables, conditions=None, columns=None,
                        distinct=False,
                        conditions_combine='AND'):
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
        conditions:
        A list of three-tuples defining conditions, e.g.:
        [(column, condition, value), ...]
        Column must be a table column name. Condition must be a *string* of a
        valid PSQL comparison (e.g. '=', '<=', 'LIKE' etc.). Value should be in
        the correct Python form relevant to the table column. Defaults to None,
        so all rows will be returned.
    columns:
        List of column names to retrieve from the database. Defaults to None,
        which returns all available columns.
    distinct:
        Boolean value, denoting whether or not to eliminate duplicate rows from
        the query. Defaults to False (i.e. duplicate rows will NOT be
        eliminated).
    conditions_combine:
        Optional; string determining how the conditions should be combined.
        Defaults to 'AND'.

    Returns
    -------
    result:
        A numpy structured array of all joined table rows which satisfy
        conditions (if given). Individual entry elements may be called by
        column name.
    """

    if cursor is not None:
        # Get the column names from the table itself
        table_string = "SELECT DISTINCT column_name, data_type FROM" \
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
            columns, dtypes = zip(*[(table_columns[i], dtypes[i]) for
                                    i in range(len(dtypes))
                      if table_columns[i].lower()
                      in columns_lower])
        logging.debug('Found these columns with these data types:')
        logging.debug(columns)
        logging.debug(dtypes)

    distinct_str = ''
    if distinct:
        distinct_str = 'DISTINCT'
    string = 'SELECT %s %s FROM %s' % (
        distinct_str,
        "*" if columns is None else ", ".join(columns),
        ' NATURAL JOIN '.join(tables),
        )

    if conditions:
        conditions_string = generate_conditions_string(conditions,
                                                       combine=
                                                       conditions_combine)
        string += ' WHERE %s' % conditions_string

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


def count_from_joined(cursor, tables, conditions=None,
                      distinct=False,
                      conditions_combine='AND'):
    """
    Extract rows from a database table join.

    Parameters
    ----------
    cursor:
        The psycopg2 cursor that interacts with the relevant database.
    tables:
        List of table names to be joined. Tables are joined using NATURAL JOIN,
        which requires that the join-ing column have the same name in ea
        table. If this is not possible, some other solution will need to be
        implemented.
    conditions:
        conditions:
        A list of three-tuples defining conditions, e.g.:
        [(column, condition, value), ...]
        Column must be a table column name. Condition must be a *string* of a
        valid PSQL comparison (e.g. '=', '<=', 'LIKE' etc.). Value should be in
        the correct Python form relevant to the table column. Defaults to None,
        so all rows will be returned.
    distinct:
        Boolean value, denoting whether or not to eliminate duplicate rows from
        the query. Defaults to False (i.e. duplicate rows will NOT be
        eliminated).
    conditions_combine:
        Optional; string determining how the conditions should be combined.
        Defaults to 'AND'.

    Returns
    -------
    result:
        A numpy structured array of all joined table rows which satisfy
        conditions (if given). Individual entry elements may be called by
        column name.
    """

    distinct_str = ''
    if distinct:
        distinct_str = 'DISTINCT'
    string = 'SELECT COUNT(%s *) FROM %s' % (
        distinct_str,
        ' NATURAL JOIN '.join(tables),
        )

    if conditions:
        conditions_string = generate_conditions_string(conditions,
                                                       combine=
                                                       conditions_combine)
        string += ' WHERE %s' % conditions_string

    logging.debug(string)

    if cursor is not None:
        cursor.execute(string)
        result = cursor.fetchall()
        logging.debug("Extract successful")
    else:
        result = None
        return result

    return int(result[0][0])


def count_grouped_from_joined(cursor, tables,
                              group_by,
                              conditions=None,
                              conditions_combine='AND',
                              case_conditions=None,
                              case_conds_combine='AND'):
    """
    Count the number of rows matching the conditions, grouped by the group_by
    column.

    Parameters
    ----------
    cursor:
        psycopg2 cursor for interacting with the database.
    tables:
        Tables that should be joined together (using NATURAL JOIN) to
        find the minimum from.
    group_by:
        The name of the table column to group the aggregate function against.
    conditions:
        conditions:
        A list of three-tuples defining conditions, e.g.:
        [(column, condition, value), ...]
        Column must be a table column name. Condition must be a *string* of a
        valid PSQL comparison (e.g. '=', '<=', 'LIKE' etc.). Value should be in
        the correct Python form relevant to the table column. The exception is
        looking for columns with value NULL, which should be denoted using the
        string 'NULL'. Defaults to None, so all rows will be returned.
    conditions_combine:
        Optional; string determining how the conditions should be combined.
        Defaults to 'AND'.
    case_conditions:
        Special case-wise conditions. Should be a list of case-wise
        conditions, each taking the following form:
        - A four-tuple containing:
            - The column that the case-wise condition applies to;
            - The comparison operator to be used;
            - A list of 4-tuples, each containing:
                - The column to be checked for this case;
                - The comparison operator to be used for this case;
                - The comparison value to be used for this case;
                - The value to return in this case;
            - The ELSE value to be used.
        The four-tuple may also be a six-tuple, with the first and last
        elements being other formatting characters (e.g. brackets).


    Returns
    -------
    The minimum value of agg_column found, cast as the appropriate Python data
    type.
    """
    logging.debug('Getting grouped aggregate from database')

    if group_by is None:
        return count_from_joined(cursor, tables,
                                 conditions=conditions,
                                 conditions_combine=conditions_combine)

    if cursor is not None:
        # Get the column names from the table itself
        table_string = "SELECT DISTINCT column_name, data_type FROM" \
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
        columns_lower = [x.lower() for x in [group_by, ]]
        columns, dtypes = zip(*[(table_columns[i], dtypes[i]) for
                                i in range(len(dtypes)) if
                                table_columns[i].lower() in
                                columns_lower])
        logging.debug('Found these columns with these data types:')
        logging.debug(columns)
        logging.debug(dtypes)

    if not isinstance(tables, list):
        tables = [tables, ]

    # Aggregate function
    query_string = 'SELECT %s, COUNT(*) FROM ' % (group_by, )

    # Table join
    query_string += ' NATURAL JOIN '.join(tables)

    if conditions or case_conditions:
        query_string += ' WHERE'

    # Conditions
    if conditions:
        conditions_string = generate_conditions_string(conditions,
                                                       combine=
                                                       conditions_combine)
        query_string += conditions_string

    if case_conditions:
        case_conds_string = generate_case_conditions_string(case_conditions,
                                                            combine=
                                                            case_conds_combine)
        query_string += ' AND %s' % case_conds_string

    # logging.debug(query_string)

    query_string += ' GROUP BY %s' % group_by

    logging.debug(query_string)

    if cursor is not None:
        cursor.execute(query_string)
        result = cursor.fetchall()

        # Re-format the return as a numpy structured array
        # Note that, because we know exactly what is coming back, we can
        # code this directly rather than detecting columns

        result = np.asarray(result, dtype={
            "names": [group_by, 'count', ],
            "formats": [psql_to_numpy_dtype(dtype) for dtype in
                        dtypes + ('bigint', )],
        })
        return result

    return None


def extract_from_left_joined(cursor, tables, join_on_column,
                             conditions=None, columns=None,
                             conditions_combine='AND',
                             distinct=False):
    """
    Extract rows from a database table join made using the LEFT JOIN construct.
    LEFT JOIN will result all table rows from the left table(s) in the query,
    even if they match no rows on the right side of the join.

    Parameters
    ----------
    cursor:
        The psycopg2 cursor that interacts with the relevant database.
    tables:
        List of table names to be joined. Tables are joined 'blindly' with
        LEFT JOIN, which requires that the join-ing column have the same name
        in each table. If this is not possible, some other solution will need
        to be implemented.
    join_on_column:
        The table column(s) to join the tables on. This can either be a single
        column name, or a list of column names, each column corresponding
        to the column to join those two tables (so, the first element of
        join_on_column will be used to join the first and second tables in
        tables, the second element will be used to join the second and third
        tables, and so on). The length of the list must be one less than the
        length of the tables list.
        Note that if a join_on_column is incorrectly placed or does not exist,
        psycopg2 will raise a ProgrammingError.
    conditions:
        conditions:
        A list of three-tuples defining conditions, e.g.:
        [(column, condition, value), ...]
        Column must be a table column name. Condition must be a *string* of a
        valid PSQL comparison (e.g. '=', '<=', 'LIKE' etc.). Value should be in
        the correct Python form relevant to the table column. The exception is
        looking for columns with value NULL, which should be denoted using the
        string 'NULL'. Defaults to None, so all rows will be returned.
    columns:
        List of column names to retrieve from the database. Defaults to None,
        which returns all available columns.
    distinct:
        Boolean value, denoting whether or not to eliminate duplicate rows from
        the query. Defaults to False (i.e. duplicate rows will NOT be
        eliminated).
    conditions_combine:
        Optional; string determining how the conditions should be combined.
        Defaults to 'AND'.

    Returns
    -------
    result:
        A numpy structured array of all joined table rows which satisfy
        conditions (if given). Individual entry elements may be called by
        column name.
    """
    if columns is not None:
        logging.debug('Passed in columns: %s' % ', '.join(columns))

    if not isinstance(join_on_column, list):
        # Singular value was passed - expand into array of options
        join_on_column = [join_on_column] * (len(tables) - 1)
    if len(join_on_column) != (len(tables) - 1):
        raise ValueError('join_on_column must have one less element '
                         'than tables')

    if cursor is not None:
        # Get the column names from the table itself
        table_string = "SELECT DISTINCT column_name, data_type FROM" \
                       " information_schema.columns " \
                       "WHERE table_name IN (%s)" %\
                       (','.join(map(lambda x: "'%s'" % x, tables)), )
        logging.debug(table_string)
        cursor.execute(table_string)
        table_structure = cursor.fetchall()
        try:
            table_columns, dtypes = zip(*table_structure)
            logging.debug('All available columns: %s' %
                          ', '.join(table_columns))
        except ValueError:
            # This occurs when one of the tables in empty
            logging.info('At least one of the requested tables has no columns')
            return []

        if columns is None:
            columns = table_columns
        else:
            columns_lower = [x.lower() for x in columns]
            logging.debug('Searching for dtypes for columns: %s' %
                          ', '.join(columns_lower))
            columns, dtypes = zip(*[(table_columns[i], dtypes[i]) for
                                    i in range(len(dtypes))
                                    if table_columns[i].lower()
                                    in columns_lower])
        logging.debug('Found these columns with these data types:')
        logging.debug(columns)
        logging.debug(dtypes)

    table_string = ' '.join(['LEFT JOIN {1} ON ({0}.{2} = {1}.{2})'.format(
        tables[i-1], tables[i], join_on_column[i-1]
    ) for i in range(1, len(tables))])

    # Need to prepend the first table name to the join_on_column value to
    # avoid ambiguity errors
    columns = list(columns)
    for j in range(len(join_on_column)):
        for i in range(len(columns)):
            if join_on_column[j].lower() == columns[i]:
                columns[i] = '%s.%s' % (tables[j], columns[i], )

    distinct_str = ''
    if distinct:
        distinct_str = 'DISTINCT'
    string = 'SELECT %s %s FROM %s' % (
        distinct_str,
        "*" if columns is None else ", ".join(columns),
        '%s %s' % (tables[0], table_string, )
        )

    if conditions:
        conditions_string = generate_conditions_string(conditions,
                                                       combine=
                                                       conditions_combine)
        string += ' WHERE %s' % conditions_string

    logging.debug(string)

    if cursor is not None:
        cursor.execute(string)
        result = cursor.fetchall()
        logging.debug("Extract successful")
    else:
        result = None
        return result

    # Now need to strip the table name off the join_on_column:
    for j in range(len(join_on_column)):
        for i in range(len(columns)):
            if '.%s' % join_on_column[j].lower() in columns[i]:
                columns[i] = join_on_column[j].lower()

    # Re-format the result as a structured numpy table
    result = np.asarray(result, dtype={
        "names": columns,
        "formats": [psql_to_numpy_dtype(dtype) for dtype in dtypes],
        })

    return result


def select_agg_from_joined(cursor, tables, aggregate, agg_column,
                           conditions=None,
                           conditions_combine='AND'):
    """
    Select the aggregate value of a particular column, subject to certain
    conditions. Table joins allow for expanded criteria to be used.

    Parameters
    ----------
    cursor:
        psycopg2 cursor for interacting with the database.
    tables:
        Tables that should be joined together (using NATURAL JOIN) to
        find the minimum from.
    aggregate:
        The PostGres aggregate function to be passed. A ProgrammingError will
        be raised if the function is not valid. Pass the function name ONLY -
        no special characters or brackets are necessary
    agg_column:
        The name of the table column to find the minimum value for.
    conditions:
        conditions:
        A list of three-tuples defining conditions, e.g.:
        [(column, condition, value), ...]
        Column must be a table column name. Condition must be a *string* of a
        valid PSQL comparison (e.g. '=', '<=', 'LIKE' etc.). Value should be in
        the correct Python form relevant to the table column. The exception is
        looking for columns with value NULL, which should be denoted using the
        string 'NULL'. Defaults to None, so all rows will be returned.
    conditions_combine:
        Optional; string determining how the conditions should be combined.
        Defaults to 'AND'.

    Returns
    -------
    The minimum value of agg_column found, cast as the appropriate Python data
    type.
    """
    logging.debug('Getting aggregate from database')

    if not isinstance(tables, list):
        tables = [tables, ]

    # Aggregate function
    query_string = 'SELECT %s(%s) FROM ' % (aggregate, agg_column,)

    # Table join
    query_string += ' NATURAL JOIN '.join(tables)

    # Conditions
    if conditions:
        conditions_string = generate_conditions_string(conditions,
                                                       combine=
                                                       conditions_combine)
        query_string += ' WHERE %s' % conditions_string

    logging.debug(query_string)

    if cursor is not None:
        cursor.execute(query_string)
        result = cursor.fetchall()
        try:
            minval = result[0][0]
            return minval
        except IndexError:
            # Must not be a valid value, return None
            return None

    return None


def select_group_agg_from_joined(cursor, tables, aggregate, agg_column,
                                 group_by,
                                 conditions=None,
                                 conditions_combine='AND'):
    """
    Select the aggregate value of a particular column, subject to certain
    conditions, grouped by values of another column. Table joins allow for
    expanded criteria to be used.

    Parameters
    ----------
    cursor:
        psycopg2 cursor for interacting with the database.
    tables:
        Tables that should be joined together (using NATURAL JOIN) to
        find the minimum from.
    aggregate:
        The PostGres aggregate function to be passed. A ProgrammingError will
        be raised if the function is not valid. Pass the function name ONLY -
        no special characters or brackets are necessary
    agg_column:
        The name of the table column to find the minimum value for.
    group_by:
        The name of the table column to group the aggregate function against.
    conditions:
        conditions:
        A list of three-tuples defining conditions, e.g.:
        [(column, condition, value), ...]
        Column must be a table column name. Condition must be a *string* of a
        valid PSQL comparison (e.g. '=', '<=', 'LIKE' etc.). Value should be in
        the correct Python form relevant to the table column. The exception is
        looking for columns with value NULL, which should be denoted using the
        string 'NULL'. Defaults to None, so all rows will be returned.
    conditions_combine:
        Optional; string determining how the conditions should be combined.
        Defaults to 'AND'.

    Returns
    -------
    The minimum value of agg_column found, cast as the appropriate Python data
    type.
    """
    logging.debug('Getting grouped aggregate from database')

    if group_by is None:
        return select_agg_from_joined(cursor, tables, aggregate, agg_column,
                                      conditions=conditions,
                                      conditions_combine=conditions_combine)

    if cursor is not None:
        # Get the column names from the table itself
        table_string = "SELECT DISTINCT column_name, data_type FROM" \
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
        columns_lower = [x.lower() for x in [group_by, agg_column, ]]
        columns, dtypes = zip(*[(table_columns[i], dtypes[i]) for
                                i in range(len(dtypes)) if
                                table_columns[i].lower() in
                                columns_lower])
        # columns = columns_lower
        dtypes = {columns[i]: dtypes[i] for i in range(len(columns))}
        logging.debug('Found these columns with these data types:')
        logging.debug(columns)
        logging.debug(dtypes)

    if not isinstance(tables, list):
        tables = [tables, ]

    # Aggregate function
    query_string = 'SELECT %s, %s(%s) FROM ' % (group_by,
                                                aggregate,
                                                agg_column, )

    # Table join
    query_string += ' NATURAL JOIN '.join(tables)

    # Conditions
    if conditions:
        conditions_string = generate_conditions_string(conditions,
                                                       combine=
                                                       conditions_combine)
        query_string += ' WHERE %s' % conditions_string

    query_string += ' GROUP BY %s' % group_by

    logging.debug(query_string)

    if cursor is not None:
        cursor.execute(query_string)
        result = cursor.fetchall()

        # Re-format the return as a numpy structured array
        # Note that, because we know exactly what is coming back, we can
        # code this directly rather than detecting columns

        result = np.asarray(result, dtype={
            "names": [group_by, agg_column, ],
            "formats": [psql_to_numpy_dtype(dtype) for dtype in
                        [dtypes[group_by], dtypes[agg_column], ]],
        })
        return result

    return None


def select_min_from_joined(cursor, tables, min_column,
                           conditions=None,
                           conditions_combine='AND'):
    """
    Select the minimum value of a particular column, subject to certain
    conditions. Table joins allow for expanded criteria to be used. This is a
    special implementation that avoids PostGres necessarily scanning the entire
    database.

    Parameters
    ----------
    cursor:
        psycopg2 cursor for interacting with the database.
    tables:
        Tables that should be joined together (using NATURAL JOIN) to
        find the minimum from.
    min_column:
        The name of the table column to find the minimum value for.
    conditions:
        conditions:
        A list of three-tuples defining conditions, e.g.:
        [(column, condition, value), ...]
        Column must be a table column name. Condition must be a *string* of a
        valid PSQL comparison (e.g. '=', '<=', 'LIKE' etc.). Value should be in
        the correct Python form relevant to the table column. The exception is
        looking for columns with value NULL, which should be denoted using the
        string 'NULL'. Defaults to None, so all rows will be returned.
    conditions_combine:
        Optional; string determining how the conditions should be combined.
        Defaults to 'AND'.

    Returns
    -------
    The minimum value of min_column found, cast as the appropriate Python data
    type.
    """
    logging.debug('Getting min from database')

    if not isinstance(tables, list):
        tables = [tables, ]

    # Aggregate function
    query_string = 'SELECT %s FROM ' % (min_column, )

    # Table join
    query_string += ' NATURAL JOIN '.join(tables)

    # Conditions
    if conditions:
        conditions_string = generate_conditions_string(conditions,
                                                       combine=
                                                       conditions_combine)
        query_string += ' WHERE %s' % conditions_string

    query_string += ' ORDER BY %s ASC LIMIT 1' % (min_column, )

    logging.debug(query_string)

    if cursor is not None:
        cursor.execute(query_string)
        result = cursor.fetchall()
        try:
            minval = result[0][0]
            return minval
        except IndexError:
            # Must not be a valid value, return None
            return None

    return None


def select_max_from_joined(cursor, tables, max_column,
                           conditions=None,
                           conditions_combine='AND'):
    """
    Select the minimum value of a particular column, subject to certain
    conditions. Table joins allow for expanded criteria to be used. This is a
    special implementation that avoids PostGres necessarily scanning the entire
    database.

    Parameters
    ----------
    cursor:
        psycopg2 cursor for interacting with the database.
    tables:
        Tables that should be joined together (using NATURAL JOIN) to
        find the minimum from.
    max_column:
        The name of the table column to find the minimum value for.
    conditions:
        conditions:
        A list of three-tuples defining conditions, e.g.:
        [(column, condition, value), ...]
        Column must be a table column name. Condition must be a *string* of a
        valid PSQL comparison (e.g. '=', '<=', 'LIKE' etc.). Value should be in
        the correct Python form relevant to the table column. The exception is
        looking for columns with value NULL, which should be denoted using the
        string 'NULL'. Defaults to None, so all rows will be returned.
    conditions_combine:
        Optional; string determining how the conditions should be combined.
        Defaults to 'AND'.

    Returns
    -------
    The minimum value of max_column found, cast as the appropriate Python data
    type.
    """
    logging.debug('Getting max from database')

    if not isinstance(tables, list):
        tables = [tables, ]

    # Aggregate function
    query_string = 'SELECT %s FROM ' % (max_column,)

    # Table join
    query_string += ' NATURAL JOIN '.join(tables)

    # Conditions
    if conditions:
        conditions_string = generate_conditions_string(conditions,
                                                       combine=
                                                       conditions_combine)
        query_string += ' WHERE %s' % conditions_string

    query_string += ' ORDER BY %s DESC LIMIT 1' % (max_column,)

    logging.debug(query_string)

    if cursor is not None:
        cursor.execute(query_string)
        result = cursor.fetchall()
        try:
            maxval = result[0][0]
            return maxval
        except IndexError:
            # Must not be a valid value, return None
            return None

    return None


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
    logging.debug("Found %d rows" % len(result))
    return result


