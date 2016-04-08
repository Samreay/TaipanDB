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
    Convert psql data type to numpy data type
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
    return PSQL_TO_NUMPY_DTYPE(psql_dtype)



def create_tables(cursor, tables_dir):
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


def extract_from(cursor, table, columns=None):
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
