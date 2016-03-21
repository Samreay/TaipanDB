import imp
import os


def update(cursor, filename):

    loadCentroids = imp.load_source('loadCentroids', filename + os.sep + 'loadCentroids.py')
    loadCentroids.execute(cursor)

    loadGuides = imp.load_source('loadGuides', filename + os.sep + 'loadGuides.py')
    loadGuides.execute(cursor)

    loadStandards = imp.load_source('loadStandards', filename + os.sep + 'loadStandards.py')
    loadStandards.execute(cursor)

    loadScience = imp.load_source('loadScience', filename + os.sep + 'loadScience.py')
    loadScience.execute(cursor)
