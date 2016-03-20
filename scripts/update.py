from connection import get_connection
from create import create_tables, insert_into
import os
import logging
import imp


def update_database(connection):
    dirname = os.path.dirname(__file__)
    version_dir = os.path.abspath(dirname + "/../resources")
    logging.info("Checking for versions in %s" % version_dir)

    versions = os.listdir(version_dir)

    logging.info("Found versions %s" % versions)

    current_version = get_current_version(connection)

    if current_version not in versions:
        versions.append(current_version)

    versions.sort(key=lambda s: map(int, s.split('.')))
    versions_needed_to_update = versions[versions.index(current_version) + 1:]
    logging.info("Updating from version %s through versions %s" % (current_version, versions_needed_to_update))
    for v in versions_needed_to_update:
        update_to_version(connection, version_dir + os.sep + v)


def get_current_version(connection):
    if connection is None:
        return "0.0.0"
    cursor = connection.cursor()
    query = "SELECT version FROM version v ORDER BY v.date DESC LIMIT 1"
    cursor.execute(query)
    result = cursor.fetchall()
    assert len(result) == 1, "Expected one row, but received result of %s" % result
    return result[0]


def update_to_version(connection, version_dir):
    logging.info("Updating to version %s" % os.path.basename(version_dir))

    table_dir = version_dir + os.sep + "tables"

    if connection is None:
        cursor = None
    else:
        cursor = connection.cursor()

    if os.path.exists(table_dir):
        create_tables(cursor, table_dir)

    insert_into(cursor, "version", os.path.basename(version_dir), columns=["version"])

    execute_file = version_dir + os.sep + "misc" + os.sep + "execute.py"
    if os.path.exists(execute_file):
        execute = imp.load_source('execute', execute_file)
        execute.update(cursor, os.path.dirname(execute_file))

    if cursor is not None:
        cursor.commit()

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    # connection = get_connection()
    connection = None
    update_database(connection)
