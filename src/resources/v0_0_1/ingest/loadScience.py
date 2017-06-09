import logging
from astropy.table import Table
from src.scripts.create import insert_many_rows
from taipan.core import polar2cart
import numpy as np
import sys
import datetime
import traceback

from src.scripts.connection import get_connection


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
                          row['Jmag_Vega'],
                          row['extBV'], row['glat'], row['glon'],
                          bool(row['is_nircol_selected']),
                          bool(row['is_optLRG_selected']),
                          bool(row['is_iband_selected']),
                          bool(
                              (156. < row['ra'] <= 225. and
                               -5 < row['dec'] < 4) or
                              (225. < row['ra'] < 238. and
                               -3. < row['dec'] < 4.) or
                              ((row['ra'] > 329.5 or row['ra'] < 53.5) and
                               -35.6 < row['dec'] < -25.7)
                          ),  # Compute if target is in KiDS regions
                          bool(row['is_prisci_vpec_target']),
                          bool(row['is_full_vpec_target'])] for
                         row in science_table]
        columns2 = ["TARGET_ID", "IS_H0_TARGET", "IS_VPEC_TARGET",
                    "IS_LOWZ_TARGET", "ZSPEC", "COL_GI", "COL_JK", "MAG_J",
                    "EBV", "GLAT", "GLON",
                    "IS_NIR", "IS_LRG", "IS_IBAND",
                    "IS_LOWZ_TARGET",
                    "IS_PRISCI_VPEC_TARGET",
                    "IS_FULL_VPEC_TARGET"]
    elif science_file.split('/')[-1] in [
        'Taipan_mock_inputcat_v1.2_170303.fits',
        'Taipan_mock_inputcat_v1.3_170504.fits']:
        values_table1 = [[row['uniqid'],
                          float(row['ra']), float(row['dec']),
                          True, False, False,
                          ] + list(polar2cart((row['ra'], row['dec']))) for
                         row in science_table]
        columns1 = ["TARGET_ID", "RA", "DEC", "IS_SCIENCE", "IS_STANDARD",
                    "IS_GUIDE", "UX", "UY", "UZ"]
        values_table2 = [[row['uniqid'],
                          False, False, # False,
                          row['z_obs'],
                          row['gmag'] - row['imag'],
                          row['Jmag_Vega'],
                          row['Jmag_Vega'] - row['Kmag_Vega'] + 0.2,
                          row['extBV'], row['glat'],
                          bool(row['is_nircol_selected']),
                          bool(row['is_optLRG_selected']),
                          bool(row['is_iband_selected']),
                          bool(
                              (156. < row['ra'] <= 225. and
                               -5 < row['dec'] < 4.) or
                              (225. < row['ra'] < 238. and
                               -3. < row['dec'] < 4.) or
                              ((row['ra'] > 329.5 or row['ra'] < 53.5) and
                               -35.6 < row['dec'] < -25.7) and
                              row['is_iband_selected']
                          ),  # Compute if target is in KiDS regions
                          bool(row['is_prisci_vpec_target']),
                          bool(row['is_full_vpec_target']),
                          bool(row['has_sdss_zspec']),
                          bool(row['has_sdss_zspec'] and
                               not row['is_prisci_vpec_target'])] for
                         row in science_table]
        columns2 = ["TARGET_ID", "IS_H0_TARGET", "IS_VPEC_TARGET",
                    # "IS_LOWZ_TARGET",
                    "ZSPEC", "COL_GI", "MAG_J", "COL_JK",
                    "EBV", "GLAT",
                    "IS_NIR", "IS_LRG", "IS_IBAND",
                    "IS_LOWZ_TARGET",
                    "IS_PRISCI_VPEC_TARGET",
                    "IS_FULL_VPEC_TARGET",
                    "HAS_SDSS_ZSPEC", "SUCCESS"]
    elif science_file.split('/')[-1] in [
        'Taipan_mock_inputcat_v2.0_170518.fits']:
        values_table1 = [[row['uniqid'],
                          float(row['ra']), float(row['dec']),
                          True, False, False,
                          ] + list(polar2cart((row['ra'], row['dec']))) for
                         row in science_table]
        columns1 = ["TARGET_ID", "RA", "DEC", "IS_SCIENCE", "IS_STANDARD",
                    "IS_GUIDE", "UX", "UY", "UZ"]
        values_table2 = [[row['uniqid'],
                          False, False, # False,
                          row['z_obs'],
                          row['gmag'] - row['imag'],
                          row['Jmag_Vega'],
                          row['Jmag_Vega'] - row['Kmag_Vega'] + 0.2,
                          row['extBV'], row['glat'],
                          bool(row['is_nircol_selected']),
                          bool(row['is_optLRG_selected']),
                          bool(row['is_iband_selected']),
                          bool((
                                   row['is_in_kids_region'] and
                                   row['is_iband_selected']) or
                               row['is_sdss_legacy_target']
                          ),  # Compute if target is in KiDS regions
                          bool(row['is_sdss_legacy_target']),
                          bool(row['is_prisci_vpec_target']),
                          bool(row['is_full_vpec_target']),
                          bool(row['has_sdss_spectrum']),
                          bool((row['has_sdss_spectrum'] or (
                              row['has_literature_zspec'] and
                              row['z_obs'] > 0.1
                          )
                                ) and
                               not row['is_prisci_vpec_target'])] for
                         row in science_table]
        columns2 = ["TARGET_ID", "IS_H0_TARGET", "IS_VPEC_TARGET",
                    # "IS_LOWZ_TARGET",
                    "ZSPEC", "COL_GI", "MAG_J", "COL_JK",
                    "EBV", "GLAT",
                    "IS_NIR", "IS_LRG", "IS_IBAND",
                    "IS_LOWZ_TARGET",
                    "IS_SDSS_LEGACY",
                    "IS_PRISCI_VPEC_TARGET",
                    "IS_FULL_VPEC_TARGET",
                    "HAS_SDSS_ZSPEC", "SUCCESS"]
    else:
        logging.info("I don't know the structure of this file %s - aborting" %
                     science_file)
        return

    logging.info('Loading science targets to cursor')
    # Insert into database
    if cursor is not None:
        insert_many_rows(cursor, "target", values_table1, columns=columns1)
        insert_many_rows(cursor, "science_target", values_table2,
                         columns=columns2)
        logging.info("Loaded Science")
    else:
        logging.info("No database - however, dry-run of loading successful")


if __name__ == '__main__':
    global_start = datetime.datetime.now()

    # Override the sys.excepthook behaviour to log any errors
    # http://stackoverflow.com/questions/6234405/logging-uncaught-exceptions-in-python
    def excepthook_override(exctype, value, tb):
        # logging.error(
        #     'My Error Information\nType: %s\nValue: %s\nTraceback: %s' %
        #     (exctype, value, traceback.print_tb(tb), ))
        # logging.error('Uncaught error/exception detected',
        #               exctype=(exctype, value, tb))
        logging.critical(''.join(traceback.format_tb(tb)))
        logging.critical('{0}: {1}'.format(exctype, value))
        # logging.error('Type:', exctype)
        # logging.error('Value:', value)
        # logging.error('Traceback:', tb)
        return
    sys.excepthook = excepthook_override

    # Set the logging to write to terminal AND file
    logging.basicConfig(
        level=logging.INFO,
        filename='sciloadmanual.log',
        filemode='w'
    )
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logging.info('Manually inserting science targets only')

    # Get a cursor
    logging.debug('Getting connection')
    conn = get_connection()
    cursor = conn.cursor()
    # Execute the simulation based on command-line arguments
    logging.debug('Doing execute function')
    execute(cursor,
            science_file='Taipan_mock_inputcat_v1.1_170208.fits')
    conn.commit()

    global_end = datetime.datetime.now()
    global_delta = global_end - global_start
    logging.info('')
    logging.info('--------')
    logging.info('INSERT COMPLETE COMPLETE')
    logging.info('Run time:')
    logging.info('%dh %dm %2.1fs' % (
        global_delta.total_seconds() // 3600,
        (global_delta.total_seconds() % 3600) // 60,
        global_delta.total_seconds() % 60,
    ))
