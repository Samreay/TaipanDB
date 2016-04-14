import json
import os
import psycopg2
import logging


def get_config(conf_filename="../config.json"):
    logging.info("Getting config from %s" % conf_filename)
    dir = os.path.dirname(__file__)
    if not dir:
        dir = "."
    config_file = dir + os.sep + conf_filename
    with open(config_file) as data_file:
        config = json.load(data_file)
    return config


def get_connection():
    connection = psycopg2.connect(**get_config())
    logging.info("Got database connection")
    return connection
