import logging
import os
import sys
import pandas as pd


# -----
# UTILITY FUNCTIONS
# -----
def query_yes_no(question, default="no"):
    """Ask a yes/no question via raw_input() and return their answer.

    Parameters
    ----------
    question:
        A string that is presented to the user.
    default:
        The presumed answer if the user just hits <Enter>.
        It must be "yes" (the default), "no" or None (meaning
        an answer is required of the user).

    Returns
    -------
    answer:
        True for "yes" or False for "no".
    """

    valid = { "yes": True,
              "y": True,
              "ye": True,
              "no": False,
              "n": False }

    if default is None:
        prompt = "[y/n] "
    elif default == "yes":
        prompt = "[Y/n] "
    elif default == "no":
        prompt = "[y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write('%s %s' % (question, prompt))
        choice = raw_input().lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "
                             "(or 'y' or 'n').\n")


def destroy_tables(cursor):
    """
    DROP all tables owned by the database user specified in the cursor.

    Parameters
    ----------
    cursor:
        Database cursor used for access.

    Returns
    -------
    Nil. User will be prompted to confirm that they want to blow away the
    tables.
    """

    # Confirm the deletion
    confirm = query_yes_no('Are you SURE you want to drop all tables?',
                           default='no')
    if confirm:
        confirm = query_yes_no('Are you REALLY, REALLY SURE?', default='no')
    if not confirm:
        return

    # Find out who the user is
    conndict = dict([s.split('=') for s in cursor.connection.dsn.split(' ')])

    string = 'DROP OWNED BY %s CASCADE' % (conndict['user'])
    cursor.execute(string)
    return


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

    names = sorted(os.listdir(tables_dir), key=lambda x: x.split("_")[0])
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
                string += "REFERENCES %s (%s) ON DELETE CASCADE" % \
                          (col_ref, col_name)
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


def insert_many_rows(cursor, table, values, columns=None, batch=100,
                     skip_on_conflict=False):
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
        Integer, denoting how many rows to write in each pass. Defaults to 100.
    skip_on_conflict:
        Optional; boolean, describing whether or not to skip a row if it already
        exists in the DB as determined by the table primary keys (True) or not.
        Defaults to False (i.e. attempting to insert a duplicate PK will result
        in a ProgrammingError).

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
    if skip_on_conflict:
        string += " ON CONFLICT DO NOTHING"
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


def insert_row(cursor, table, values, columns=None,
               skip_on_conflict=False):
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
    skip_on_conflict:
        Optional; boolean, describing whether or not to skip a row if it already
        exists in the DB as determined by the table primary keys (True) or not.
        Defaults to False (i.e. attempting to insert a duplicate PK will result
        in a ProgrammingError).

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
    if skip_on_conflict:
        string += " ON CONFLICT DO NOTHING"
    logging.debug(string + " with values " + str(values))
    if cursor is not None:
        cursor.execute(string, values)
        logging.debug("Insert successful")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    conn = None
    create_tables(conn)


def create_index(cursor, table, columns, ordering=None):
    """
    Create a btree index within the specified database table.

    This is an outstanding boost to performance if the standard indexing (via
    the primary key specified in the table creation) is not sufficient. The
    most obvious example is the indexing of the large 'observability' table
    by date, in addition to the standard (field_id, date) pair.

    The index will not be named by the user - the database server will use
    a sensible default name.

    Parameters
    ----------
    cursor : psycopg2.cursor object
        For communication with the database
    table : str
        The database table to be indexed
    columns : list of str, or str
        Either a list of columns for indexing, or a single column name.
    ordering : str, defaults to None
        You can specify an ordering to the index ('ASC' or 'DESC') here,
        if applicable. Defaults to None, at which point no ordering will be
        applied. Ordering can only be applied to single-column indices.

    Returns
    -------
    Nil. Database is updated in-situ.
    """
    # Input checking
    if not isinstance(columns, list):
        columns = [columns, ]

    allowed_orderings = ['ASC', 'DESC']
    if ordering is not None:
        if len(columns) > 1:
            raise ValueError('ordering may only be used when supplying a '
                             'single column to the index')
        if ordering not in allowed_orderings:
            raise ValueError('ordering must be one of %s' %
                             allowed_orderings.join(', '))

    string = "CREATE INDEX ON %s (%s %s)" % (table,
                                             columns.join(','),
                                             ordering if ordering else '')

    cursor.execute(string)

