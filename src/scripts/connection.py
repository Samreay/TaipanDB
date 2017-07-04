import json
import os
import psycopg2
import logging


def get_config(conf_filename="../../config.json"):
    """
    Load a database connection configuration file.

    Parameters
    ----------
    conf_filename:
        Filename of the configuration file. Defaults to '../../config.json'
        (which is the location of the file in the development VM).

    Returns
    -------
    config:
        A JSON string containing the configuration information.
    """
    logging.debug("Getting config from %s" % conf_filename)
    dir = os.path.dirname(__file__)
    if not dir:
        dir = "."
    config_file = dir + os.sep + conf_filename
    with open(config_file) as data_file:
        config = json.load(data_file)
    return config


def get_connection(autocommit=True):
    """
    Get a psycopg connection for interacting with the database.

    Returns
    -------
    connection:
        A psycopg2 connection object.
    """
    connection = psycopg2.connect(**get_config())
    if autocommit:
        connection.autocommit = True
    logging.debug("Got database connection")
    return connection
