import os
import psycopg2

from loguru import logger
from config import Config
from psycopg2 import OperationalError


class DB:

    cfg = None
    db_name = None
    db_user = None
    db_pass = None
    db_host = None
    db_port = None

    def __init__(self, config: Config):
        self.cfg = config

        self.db_name = self.cfg.get('db_name')
        self.db_user = self.cfg.get('db_user')
        self.db_pass = self.cfg.get('db_pass')
        self.db_host = self.cfg.get('db_host')
        self.db_port = self.cfg.get('db_port')

        self.execute_sql('drop_db')

        result = self.execute_sql('table_exists', self.db_name)
        if result and result[0] and result[0][0]:
            if result[0][0] != 1:
                self.execute_sql('create_db')

        self.execute_sql('create_table_blocks')

    def _connect(self):
        connection = None

        try:
            connection = psycopg2.connect(
                database=self.db_name,
                user=self.db_user,
                password=self.db_pass,
                host=self.db_host,
                port=self.db_port)
            connection.autocommit = True

        except OperationalError as e:
            logger.exception(f'Error while connecting to DB: {e}')

        return connection

    def _sql(self, file):
        with open(os.path.join('sql', file), 'r', encoding='utf8') as f:
            return f.read()

    # TODO: Do this better with 'with' or connection.close()?
    def execute_sql(self, name: str, *args):
        cursor = self._connect().cursor()
        query = self._sql(f'{name}.sql')

        try:
            cursor.execute(query, args)
            return cursor.fetchall()

        except OperationalError as e:
            logger.exception(f'Error while executing SQL: {e}')
