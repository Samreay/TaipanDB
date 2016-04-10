import logging
import os
import pandas as pd


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


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    conn = None
    create_tables(conn)
