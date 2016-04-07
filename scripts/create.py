import logging
import os
import pandas as pd
import numpy as np


# ------
# HELPER FUNCTIONS
# ------


# psql-numpy data type relationship
# TBC: How to handle char, varchar etc? Given they will have (n) at the 
# end of them
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
}



def create_tables(cursor, tables_dir):
    logging.info("Creating tables declare in %s" % tables_dir)

    names = sorted(os.listdir(tables_dir), cmp=lambda x, y: 1 if int(x.split("_")[0]) > int(y.split("_")[0]) else -1)
    logging.debug("Found table files: %s" % names)
    exec_strings = []
    tables = []
    for table_file in names:
        table_name = table_file.split(".")[0].partition("_")[2].replace(" ", "_").lower()
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


def insert_into(cursor, table, values, columns=None):
    if isinstance(values, list):
        if isinstance(values[0], list):
            values2 = values[0]
        else:
            values2 = values
    else:
        values2 = [values]
    string = "INSERT INTO %s %s VALUES (%s)" % (
        table,
        "" if columns is None else "(" + ", ".join(columns) + ")",
        ",".join(["%s"] * len(values2))
        )
    logging.debug(string + " with values " + str(values))
    if cursor is not None:
        cursor.execute(string, values2)
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
        "formats": [PSQL_TO_NUMPY_DTYPE[dtype] for dtype in dtypes],
        })

    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    conn = None
    create_tables(conn)
