import logging
from scripts.create import insert_into
from taipan.tiling import generate_SH_tiling


def execute(cursor, tiling_file=None):
    logging.info("Loading Centroids")

    if not tiling_file:
        logging.info("No tiling file passed - aborting loading centroids")
        return

    # Get centroids
    # We do this by creating TaipanTile objects in memory - that module already
    # has the necessary code to convert the text lists to RA, Dec tied to a
    # field
    # POSSIBLE UPDATE: Divide the current generate_SH_tiling into two functions
    # - one which parses the input file into coordinates, and the other which
    # generates TaipanTile objects
    faux_tiles = generate_SH_tiling(tiling_file, randomise_seed=False,
                                    randomise_pa=False)
    # Convert this into a table of values
    values = [[i, faux_tiles[i].ra, faux_tiles[i].dec]
              for i in range(len(faux_tiles))]
    columns = ["FIELD_ID", "RA", "DEC"]

    # Insert into database
    if cursor is not None:
        insert_into(cursor, "fields", values, columns=columns)

    logging.info("Loaded Centroids")
