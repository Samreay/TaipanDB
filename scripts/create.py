import logging
import os
import re
import pandas as pd
import numpy as np


# ------
# HELPER FUNCTIONS
# ------


# psql-numpy data type relationship
PSQL_TO_NUMPY_DTYPE = {
    "smallint": "int16",
    "integer": "int32",
    "bigint": "int64",
    "decimal": "float64",
    "numeric": "float64",
    "real": "float32",
    "double": "float64",
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
            raise ValueError('Invalid char data type passed'
                ' to psql_to_numpy_dtype')
        return 'S%s' % (match.group('len'))

    # All other types
    return PSQL_TO_NUMPY_DTYPE[psql_dtype]



def create_tables(cursor, tables_dir):
    """
    Create database tables as per the configuration file(s) in tables_dir.

    Parameters
    ----------
    cursor:
        The psycopg2 cursor to provide access to the database.
    tables_dir:
        The (relative or absolute) path to the directory containing the
        configuration file(s) to be converted into database tables.

    Returns
    -------
    Nil. Database tables created using the cursor.
    """

    logging.info("Creating tables declare in %s" % tables_dir)

    names = sorted(os.listdir(tables_dir), 
                   cmp=lambda x, y: 1 if int(
                        x.split("_")[0]
                        ) > int(y.split("_")[0]) else -1)
    logging.debug("Found table files: %s" % names)
    exec_strings = []
    tables = []
    for table_file in names:
        table_name = table_file.split(".")[
            0
            ].partition("_")[2].replace(" ", "_").lower()
        string = "CREATE TABLE %s (" % table_name
        tab = pd.read_csv(
            tables_dir + os.sep + table_file, 
            delim_whitespace=True, comment="#", dtype=str,
            header=0, quotechar='"', skipinitialspace=True)
        pks = []
        for date, column in tab.T.iteritems():
            col_name = column["name"].replace(" ", "_").lower()
            string += col_name + " "
            col_type = column["type"]
            if col_type.upper() == "DOUBLE":
                col_type = "double precision"
            string += col_type + " "
            col_null = column["nullable"]
            if col_null.upper() == "FALSE":
                string += "not null "
            col_def = column["default_value"]
            if col_def.upper() != "NONE":
                string += "default %s " % col_def
            if column["pk"].upper() == "TRUE":
                pks.append(col_name)
            col_ref = column["foreign_key_table"].lower()
            if column["unique"].upper() == "TRUE":
                string += "UNIQUE "
            if col_ref != "none":
                string += "REFERENCES %s (%s) " % (col_ref, col_name)
            string += ", "
        string += "PRIMARY KEY (%s)" % ",".join(pks)
        string += " );"
        assert len(pks) > 0, "Table %s has no primary keys!" % table_name
        logging.debug("Statement is %s" % string)
        exec_strings.append(string)
        tables.append(table_name)

    # Currently all tables are created at once.
    if cursor is not None:
        for s, t in zip(exec_strings, tables):
            logging.info("Creating table %s" % t)
            cursor.execute(s)
        logging.info("Created all tables")


def insert_many_rows(cursor, table, values, columns=None, batch=100):
    """
    Insert multiple rows into a database table.

    Parameters
    ----------
    cursor:
        The psycopg2 cursor that interacts with the relevant database.
    table:
        The name of the table to be manipulated.
    values:
        The values to be added to the table, as a list of lists (although
        it should work for an iterable of iterables). In each sub-list, values
        should be in the order of the database columns, unless the columns
        argument is also passed; in that case, values should be in order
        corresponding to the columns parameter.
    columns:
        List of column names that correspond to the ordering of values. Can also
        be used to restrict the number of columns to write to (i.e. allow
        default table values for columns if not required). Defaults to None,
        which assumes that you wish to write information to all columns.
    batch:
        Interger, denoting how many rows to write in each pass. Defaults to 100.

    Returns
    -------
    Nil. Cursor writes values to table.
    """

    values = [tuple(v) for v in values]
    index = 0
    string = "INSERT INTO %s %s VALUES %s" % (
        table,
        "" if columns is None else "(" + ", ".join(columns) + ")",
        "%s"
    )
    logging.debug("MANY ROW INSERT: " + string 
                  + "... total of %d elements" % len(values))

    if cursor is not None:
        while index < len(values):
            end = index + batch
            if end > len(values):
                end = len(values)
            rows = values[index:end]
            index = end
            current_string = string % ",".join(["%s"] * len(rows))
            cursor.execute(current_string, rows)


def insert_row(cursor, table, values, columns=None):
    """
    Insert a row into a database table.

    Parameters
    ----------
    cursor:
        The psycopg2 cursor that interacts with the relevant database.
    table:
        The name of the table to be manipulated.
    values:
        The values to be added to the table, as a list. Values
        should be in the order of the database columns, unless the columns
        argument is also passed; in that case, values should be in order
        corresponding to the columns parameter.
    columns:
        List of column names that correspond to the ordering of values. Can also
        be used to restrict the number of columns to write to (i.e. allow
        default table values for columns if not required). Defaults to None,
        which assumes that you wish to write information to all columns.

    Returns
    -------
    Nil. Cursor writes values to table.
    """
    if isinstance(values, list):
        if isinstance(values[0], list):
            raise ValueError("A nested list should"
                             " call the insert_many function")
    else:
        values = [values]
    string = "INSERT INTO %s %s VALUES (%s)" % (
        table,
        "" if columns is None else "(" + ", ".join(columns) + ")",
        ",".join(["%s"] * len(values))
        )
    logging.debug(string + " with values " + str(values))
    if cursor is not None:
        cursor.execute(string, values)
        logging.debug("Insert successful")


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
        table_columns, dtypes = zip(*table_structure)
        if columns is None:
            columns = table_columns
        else:
            columns_lower = [x.lower() for x in columns]
            dtypes = [dtypes[i] for i in range(len(dtypes))
                      if table_columns[i].lower() 
                      in columns_lower]
    
    string = "SELECT %s FROM %s" % (
        "*" if columns is None else "(" + ", ".join(columns) + ")",
        table,
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
        cursor.execute("SELECT column_name, data_type"
            " FROM information_schema.columns"
            " WHERE table_name LIKE '%s'" % ('|'.join(tables), ))
        tables_structure = cursor.fetchall()
        table_structure = cursor.fetchall()
        table_columns, dtypes = zip(*table_structure)
        if columns is None:
            columns = table_columns
        else:
            columns_lower = [x.lower() for x in columns]
            dtypes = [dtypes[i] for i in range(len(dtypes))
                      if table_columns[i].lower() 
                      in columns_lower]
    string = 'SELECT %s FROM %s' % (
        "*" if columns is None else "(" + ", ".join(columns) + ")",
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



if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    conn = None
    create_tables(conn)
