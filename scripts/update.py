from connection import get_connection
from create import create_tables
import os
import logging


def update_database(connection):
    dirname = os.path.dirname(__file__)
    version_dir = os.path.abspath(dirname + "/../resources")
    logging.info("Checking for versions in %s" % version_dir)

    versions = os.listdir(version_dir)
    versions.sort(key=lambda s: map(int, s.split('.')))

    logging.info("Found versions %s" % versions)
    for v in versions:
        update_to_version(connection, version_dir + os.sep + v)


def update_to_version(connection, version_dir):
    logging.info("Updating to version %s" % os.path.basename(version_dir))

    table_dir = version_dir + os.sep + "tables"

    if os.path.exists(table_dir):
        create_tables(connection, table_dir)

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    connection = None
    update_database(connection)
