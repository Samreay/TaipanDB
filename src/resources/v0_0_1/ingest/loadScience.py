import logging
from astropy.table import Table
from ....scripts.create import insert_many_rows
from taipan.core import polar2cart
import numpy as np


def execute(cursor, science_file=None, mark_active=True):
    """
    Load science targets from file into the database.

    Parameters
    ----------
    cursor:
        psycopg2 cursor for interacting with the database.
    science_file:
        File to load the science targets from. Defaults to None, at which point
        the function will abort.

    Returns
    -------
    Nil. Science targets are loaded into the database.
    """
    logging.info("Loading Science")

    if not science_file:
        logging.info("No file passed - aborting loading science")
        return

    # Get science
    science_table = Table.read(science_file)

    # FOR TEST/FUDGE USE ONLY
    # Step through the table, and where we find a duplicate ID, alter it
    # by adding id*1e9

    # Do some stuff to convert science_table into values_table
    # (This is dependent on the structure of science_file)
    logging.debug('Creating tables for database load')
    if science_file.split('/')[-1] == 'priority_science.v0.101_20160331.fits':
        values_table1 = [[row['uniqid'] + int(1e9)*row['reference'],
                          float(row['ra']), float(row['dec']),
                          True, False, False,
                          mark_active]
                          + list(polar2cart((row['ra'], row['dec'])))
                         for row in science_table]
        columns1 = ["TARGET_ID", "RA", "DEC", "IS_SCIENCE", "IS_STANDARD",
                    "IS_GUIDE", "IS_ACTIVE", "UX", "UY", "UZ"]
        values_table2 = [[row['uniqid'] + int(1e9)*row['reference'],
                          row['priority'],
                          bool(row['is_H0']), bool(row['is_vpec']),
                          bool(row['is_lowz'])]
                         for row in science_table]
        columns2 = ["TARGET_ID", "PRIORITY", "IS_H0_TARGET", "IS_VPEC_TARGET",
                    "IS_LOWZ_TARGET"]
    elif science_file.split('/')[-1] == 'Taipan_mock_inputcat_v1.1_170208.fits':
        # This catalogue doesn't include target_ids - we need to form them
        id_start = 1001000005
        science_table['uniqid'] = np.arange(id_start,
                                            id_start+len(science_table))
        values_table1 = [[row['uniqid'],
                          float(row['ra']), float(row['dec']),
                          True, False, False,
                          ] + list(polar2cart((row['ra'], row['dec']))) for
                         row in science_table]
        columns1 = ["TARGET_ID", "RA", "DEC", "IS_SCIENCE", "IS_STANDARD",
                    "IS_GUIDE", "UX", "UY", "UZ"]
        values_table2 = [[row['uniqid'],
                          False, False, False,
                          row['z_obs'],
                          row['gmag'] - row['imag'],
                          row['Jmag_Vega'] - row['Kmag_Vega'],
                          row['extBV'], row['glat'],
                          bool(row['is_nircol_selected']),
                          bool(row['is_optLRG_selected']),
                          bool(row['is_iband_selected']),
                          bool(row['is_in_census_region']),  # new proxy for lowz
                          bool(row['is_prisci_vpec_target']),
                          bool(row['is_full_vpec_target'])] for row in science_table]
        columns2 = ["TARGET_ID", "IS_H0_TARGET", "IS_VPEC_TARGET",
                    "IS_LOWZ_TARGET", "ZSPEC", "COL_GI", "COL_JK",
                    "EBV", "GLAT",
                    "IS_NIR", "IS_LRG", "IS_IBAND",
                    "IS_LOWZ_TARGET",
                    "IS_PRISCI_VPEC_TARGET",
                    "IS_FULL_VPEC_TARGET"]
    else:
        logging.info("I don't know the structure of this file %s - aborting" %
                     science_file)
        return

    logging.debug('Loading to cursor')
    # Insert into database
    if cursor is not None:
        insert_many_rows(cursor, "target", values_table1, columns=columns1)
        insert_many_rows(cursor, "science_target", values_table2,
                         columns=columns2)
        logging.info("Loaded Science")
    else:
        logging.info("No database - however, dry-run of loading successful")
