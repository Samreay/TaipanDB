from connection import get_connection
from create import create_tables, insert_row
import os
import logging
import imp
import psycopg2
import sys


def update_database(connection):
    dirname = os.path.dirname(__file__)
    if not dirname:
        dirname = "."
    version_dir = os.path.abspath(dirname + "/../resources")
    logging.info("Checking for versions in %s" % version_dir)

    versions = os.listdir(version_dir)

    logging.info("Found versions %s" % versions)

    current_version = get_current_version(connection)

    if current_version not in versions:
        versions.append(current_version)
    versions.sort(key=lambda s: map(int, s.split('.')))
    versions_needed_to_update = versions[versions.index(current_version) + 1:]
    if len(versions_needed_to_update) == 0:
        logging.info("Database is already up to date")
        return
    logging.info("Updating from version %s through versions %s" % (current_version, versions_needed_to_update))
    for v in versions_needed_to_update:
        update_to_version(connection, version_dir + os.sep + v)


def get_current_version(connection):
    if connection is None:
        return "0.0.0"
    cursor = connection.cursor()
    query = "SELECT version FROM version v ORDER BY v.version_date DESC LIMIT 1"
    try:
        cursor.execute(query)
    except psycopg2.ProgrammingError:
        # If the relation does not exist, we are at version 0.0.0
        connection.rollback()
        return "0.0.0"
    result = cursor.fetchall()[0]
    assert len(result) == 1, "Expected one row, but received result of %s" % result
    return result[0]


def update_to_version(connection, version_dir):
    logging.info("Updating to version %s" % os.path.basename(version_dir))

    table_dir = version_dir + os.sep + "tables"

    if connection is None:
        cursor = None
    else:
        cursor = connection.cursor()
    try:
        if os.path.exists(table_dir):
            create_tables(cursor, table_dir)

        ingest_file = version_dir + os.sep + "ingest" + os.sep + "execute.py"
        if os.path.exists(ingest_file):
            execute = imp.load_source('execute', ingest_file)
            execute.update(cursor, os.path.dirname(ingest_file))

        scripts_file = version_dir + os.sep + "scripts" + os.sep + "execute.py"
        if os.path.exists(scripts_file):
            execute = imp.load_source('execute', scripts_file)
            execute.update(cursor, os.path.diranem(scripts_file))

        insert_row(cursor, "version", os.path.basename(version_dir), columns=["version"])
    except Exception as e:
        logging.critical(e)
        logging.warn("Rolling back")
        connection.rollback()
        raise
    else:
        if cursor is not None:
            logging.info("Committing. Updated to version %s" % version_dir)
            connection.commit()

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        connection = None
    else:
        connection = get_connection()
    update_database(connection)
