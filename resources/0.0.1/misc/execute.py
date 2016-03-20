from astropy.io import fits
import os
import logging


def update(cursor, filename):
    priority_science_file = filename + os.sep + "priority_science.fits"
    assert os.path.exists(priority_science_file), "File %s does not exist" % priority_science_file
    hdulist = fits.open(priority_science_file)

    ra = hdulist[1].data['ra_1']
    dec = hdulist[1].data['dec_1']

    print(ra.shape)
    # Get ux, uy, uz
    # Call insert into