import logging
import os
import pandas as pd


def create_tables(cursor, tables_dir):
    logging.info("Creating tables declare in %s" % tables_dir)

    exec_strings = []
    for table_file in os.listdir(tables_dir):
        logging.info("Creating table %s" % table_file)
        table_name = table_file.split(".")[0]
        string = "CREATE TABLE %s (" % table_name.replace(" ", "_").lower()
        tab = pd.read_csv(tables_dir + os.sep + table_file, delim_whitespace=True, comment="#", dtype=str,
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
            if col_ref != "none":
                string += "REFERENCES %s (%s) " % (col_ref, col_name)
            string += ", "
        string += "PRIMARY KEY (%s)" % ",".join(pks)
        string += " );"
        assert len(pks) > 0, "Table %s has no primary keys!" % table_name
        logging.debug("Statement is %s" % string)
        exec_strings.append(string)

    # Currently all tables are created at once.
    # We need a good way of versioning the database.
    if cursor is not None:
        for s in exec_strings:
            cursor.execute(s)
        logging.info("Created all tables")


def insert_into(cursor, table, values, columns=None):
    if type(values) == list:
        values2 = values
    else:
        values2 = [values]
    string = "INSERT INTO %s %s VALUES (%s)" % (table, "" if columns is None else "(" + ", ".join(columns) + ")",
                                                ",".join(["%s"] * len(values2)))
    logging.debug(string + " with values " + str(values))
    if cursor is not None:
        cursor.execute(string, values)
        logging.debug("Insert successful")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    conn = None
    create_tables(conn)
