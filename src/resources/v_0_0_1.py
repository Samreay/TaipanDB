from ..scripts import create
from .v0_0_1.ingest import loadGuides, loadScience, loadCentroids, loadStandards
from .v0_0_1.manipulate import makeScienceDiff, makeTargetPosn

import os


def update(cursor):
    resource_dir = os.path.dirname(__file__) + os.sep + "v0_0_1" + os.sep
    data_dir = "/data/resources/0.0.1/"
    table_dir = resource_dir + os.sep + "tables"

    create.create_tables(cursor, table_dir)

    fields_file = data_dir + "pointing_centers.radec"
    loadCentroids.execute(cursor, fields_file=fields_file)

    guides_file = data_dir + "SCOSxAllWISE.photometry.forTAIPAN." \
                             "reduced.guides_nodups.fits"
    loadGuides.execute(cursor, guides_file=guides_file)

    standards_file = data_dir + 'SCOSxAllWISE.photometry.forTAIPAN.' \
                                'reduced.standards_nodups.fits'
    loadStandards.execute(cursor, standards_file=standards_file)

    science_file = data_dir + 'priority_science.v0.101_20160331.fits'
    loadScience.execute(cursor, science_file=science_file)

    # Commit here in case something further along fails
    cursor.connection.commit()

    makeScienceDiff.execute(cursor)

    makeTargetPosn.execute(cursor)

    # Commit again
    cursor.connection.commit()
