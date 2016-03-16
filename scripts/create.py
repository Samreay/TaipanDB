from connection import get_connection
import logging


def create_target_table(cursor):
    statement = "CREATE TABLE target (target_id serial PRIMARY KEY, " \
                "ra double precision not null, " \
                "dec double precision not null, " \
                "ux double precision not null, " \
                "uy double precision not null, " \
                "uz double precision not null, " \
                "is_science boolean not null default 1, " \
                "is_guide boolean not null default 0, " \
                "is_standard boolean not null default 0," \
                "done boolean);"
    cursor.execute(statement)
    logging.info("Created the target table")


def create_field_table(cursor):
    statement = "CREATE TABLE field (field_id serial PRIMARY KEY, " \
                "ra double precision not null, " \
                "dec double precision not null, " \
                "ux double precision not null, " \
                "uy double precision not null, " \
                "uz double precision not null);"
    cursor.execute(statement)
    logging.info("Created the field table")


def create_science_target_table(cursor):
    statement = "CREATE TABLE science_targets (target_id integer not null, " \
                "is_h0_target boolean not null, " \
                "is_vpec_target boolean not null, " \
                "is_lowz_target boolean not null, " \
                "visits integer default 0, " \
                "priority integer not null, " \
                "difficulty integer not null);"
    cursor.execute(statement)
    logging.info("Created the science target table")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    conn = get_connection()
    cursor = conn.cursor()

    create_target_table(cursor)
    create_field_table(cursor)
    create_science_target_table(cursor)