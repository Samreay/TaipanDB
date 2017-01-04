# Test the values for n_sci_rem, n_sci_alloc and n_sci_obs in the
# database at any given time, using custom manual queries instead of the
# helper functions

from src.scripts.connection import get_connection

import psycopg2
import logging

def execute(cursor):
    # Need to determine the field_ids in the system
    cursor.execute('SELECT DISTINCT field_id FROM field ORDER BY field_id ASC')
    field_ids = [f[0] for f in cursor.fetchall()]

    # Loop over each field_id and compare the manually derived n_sci_* values
    # to those kept in the database
    for field_id in field_ids:
        # Get the stored info in the database for these parameters
        cursor.execute('SELECT n_sci_obs, n_sci_alloc, n_sci_rem FROM '
                       'tiling_info NATURAL JOIN tile WHERE '
                       'field_id=%s AND NOT is_observed AND NOT is_queued',
                       (field_id, )
                       )
        n_sci_obs, n_sci_alloc, n_sci_rem = cursor.fetchall()[0]

        # Now, determine each of these values with a manual query
        # n_sci_obs
        cursor.execute('SELECT COUNT(DISTINCT target_id) FROM target_posn '
                       'NATURAL JOIN science_target WHERE '
                       '(is_lowz_target OR is_h0_target OR is_vpec_target) AND '
                       'done AND field_id=%s',
                       (field_id, )
                       )
        n_sci_obs_manual = cursor.fetchall()[0][0]

        # n_sci_alloc
        cursor.execute('SELECT COUNT(DISTINCT target_id) FROM target_posn '
                       'NATURAL JOIN science_target WHERE '
                       '(is_lowz_target OR is_h0_target OR is_vpec_target) AND '
                       'field_id=%s AND target_id IN '
                       '(SELECT DISTINCT target_id FROM tile NATURAL JOIN '
                       'target_field WHERE NOT is_observed)',
                       (field_id, )
                       )
        n_sci_alloc_manual = cursor.fetchall()[0][0]

        # n_sci_rem
        cursor.execute('SELECT COUNT(DISTINCT target_id) FROM target_posn '
                       'NATURAL JOIN science_target WHERE '
                       '(is_lowz_target OR is_h0_target OR is_vpec_target) AND '
                       'field_id=%s AND NOT done AND target_id NOT IN '
                       '(SELECT DISTINCT target_id FROM tile NATURAL JOIN '
                       'target_field WHERE NOT is_observed)',
                       (field_id,)
                       )
        n_sci_rem_manual = cursor.fetchall()[0][0]

        # Do comparison
        fault_detected = False
        if n_sci_obs != n_sci_obs_manual:
            print('n_sci_obs   mismatch in field %s - db value %5d, '
                  'actual value %5d' % (field_id, n_sci_obs, n_sci_obs_manual))
            fault_detected = True
        if n_sci_alloc != n_sci_alloc_manual:
            print('n_sci_alloc mismatch in field %s - db value %5d, '
                  'actual value %5d' % (field_id, n_sci_alloc,
                                        n_sci_alloc_manual))
            fault_detected = True
        if n_sci_obs != n_sci_obs_manual:
            print('n_sci_rem   mismatch in field %s - db value %5d, '
                  'actual value %5d' % (field_id, n_sci_rem, n_sci_rem_manual))
            fault_detected = True
        if not fault_detected:
            print('All good for field %d' % field_id)

    return

if __name__ == '__main__':
    logging.debug('Getting connection')
    conn = get_connection()
    cursor = conn.cursor()

    execute(cursor)
