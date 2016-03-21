import loadCentroids
import loadGuides
import loadScience
import loadStandards


def update(cursor, filename):
    loadCentroids.execute(cursor)
    loadGuides.execute(cursor)
    loadStandards.execute(cursor)
    loadScience.execute(cursor)
