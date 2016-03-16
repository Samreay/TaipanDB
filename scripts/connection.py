import json
import os
import psycopg2
import logging


def get_config(conf_filename="cofig.json"):
    logging.info("Getting config from %s" % conf_filename)
    filename = __file__
    config_file = os.path.dirname(filename) + os.sep + conf_filename
    with open(config_file) as data_file:
        config = json.load(data_file)
    return config


def get_connection():
    connection = psycopg2.connect(**get_config())
    logging.info("Got database connection")
    return connection
