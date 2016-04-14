from .connection import get_connection
from .create import create_tables, insert_row
import os
import logging
import importlib.util
import sys
from pkg_resources import parse_version


def update_database(connection):
    dirname = os.path.dirname(__file__)
    if not dirname:
        dirname = "."
    version_dir = os.path.abspath(dirname + "/../resources")
    logging.info("Checking for versions in %s" % version_dir)

    versions = get_versions(version_dir)
    version_keys = list(versions.keys())
    print(version_keys)
    logging.info("Found versions %s" % version_keys)

    current_version = get_current_version(connection)

    if current_version not in version_keys:
        version_keys.append(current_version)
    version_keys.sort()
    versions_needed_to_update = version_keys[version_keys.index(current_version) + 1:]
    if len(versions_needed_to_update) == 0:
        logging.info("Database is already up to date")
        return
    logging.info("Updating from version %s through versions %s" % (current_version, versions_needed_to_update))
    for v in versions_needed_to_update:
        update_to_version(connection, v, versions[v])


def get_versions(version_dir):
    version_files = [v for v in os.listdir(version_dir) if os.path.isfile(version_dir + os.sep + v) and not v.endswith(".pyc") and not v.startswith("__init__")]

    modules = [__import__("src.resources." + v[:-3], fromlist=['']) for v in version_files]
    versions = [v[2:-3].replace("_", ".") for v in version_files]

    version_dict = {parse_version(k): v for k, v in zip(versions, modules)}
    return version_dict


def get_current_version(connection):
    if connection is None:
        return parse_version("0.0.0")
    cursor = connection.cursor()
    query = "SELECT version FROM version v ORDER BY v.version_date DESC LIMIT 1"
    try:
        cursor.execute(query)
    except Exception:
        # If the relation does not exist, we are at version 0.0.0
        connection.rollback()
        return parse_version("0.0.0")
    result = parse_version(cursor.fetchall()[0])
    assert len(result) == 1, "Expected one row, but received result of %s" % result
    return result[0]


def update_to_version(connection, version, version_module):
    logging.info("Updating to version %s" % version)

    if connection is None:
        cursor = None
    else:
        cursor = connection.cursor()
    try:
        version_module.update(cursor)

        insert_row(cursor, "version", os.path.basename("%s" % version), columns=["version"])
    except Exception as e:
        logging.critical(e)
        logging.warn("Rolling back")
        if connection is not None:
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
