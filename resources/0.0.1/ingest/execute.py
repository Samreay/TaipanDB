import imp
import os


def update(cursor, filename):

    loadCentroids = imp.load_source('loadCentroids', filename + os.sep + 'loadCentroids.py')
    fields_file = filename + os.sep + 'icover.3.5072.13.13.txt'
    loadCentroids.execute(cursor, fields_file=fields_file)

    loadGuides = imp.load_source('loadGuides', filename + os.sep + 'loadGuides.py')
    guides_file = filename + os.sep + 'guides.fits'
    loadGuides.execute(cursor, guides_file=guides_file)

    loadStandards = imp.load_source('loadStandards', filename + os.sep + 'loadStandards.py')
    loadStandards.execute(cursor)

    loadScience = imp.load_source('loadScience', filename + os.sep + 'loadScience.py')
    loadScience.execute(cursor)
