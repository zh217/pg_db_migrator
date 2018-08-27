import psycopg2
import logging

from .schema_resolve import get_schema_and_migrations


def get_db_version(conn):
    with conn.cursor() as cursor:
        try:
            cursor.execute("""SELECT var FROM application_metadata WHERE key='LAST_SEEN_VERSION'""")
            res, *_ = cursor.fetchall()
            logging.info(f'db application version {res[0]}')
            return res[0]
        except psycopg2.ProgrammingError:
            logging.info('db does not have metadata table')
            return None


def set_db_version(conn, cur_version):
    with conn.cursor() as cursor:
        cursor.execute("""UPDATE application_metadata SET var= %s WHERE key='LAST_SEEN_VERSION'""", (cur_version,))
        logging.info(f'set db application version {cur_version}')


def run_statements(conn, stmt):
    with conn.cursor() as cursor:
        try:
            cursor.execute(stmt)
        except Exception as e:
            logging.error('cannot execute', stmt)
            raise e


def do_migration(conn_args, base_dir, cur_version):
    init_data, schemas, migrations = get_schema_and_migrations(base_dir, cur_version)
    with psycopg2.connect(**conn_args) as conn:
        conn.autocommit = True
        if get_db_version(conn) is None:
            logging.info('running initialization script')
            logging.debug(init_data)
            run_statements(conn, init_data)
            for schema in schemas:
                logging.info('running schema creation')
                logging.debug(schema)
                run_statements(conn, schema)
        else:
            for migration in migrations:
                logging.info('running migration script')
                logging.debug(migration)
                run_statements(conn, migration)
        set_db_version(conn, cur_version)
