import logging
from scripts.create import insert_into


def execute(cursor):
    logging.info("Loading Centroids")

    # Get centroids

    # Insert into database
    # insert_into(cursor, "fields", values)

    logging.info("Loaded Centroids")
