import os
import psycopg2

from loguru import logger
from config import Config
from psycopg2 import OperationalError


# TODO: Convert to factory so that different databases can be used - based on config
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

        result = self.execute('db_exists', {'name': self.db_name})

        if result and result[0][0] != 1:
            self.execute('db_create', {'name': self.db_name})

        self.execute('blocks_create')
        self.execute('transactions_create')
        self.execute('state_change_create')
        self.execute('current_state_create')
        self.execute('contracts_create')

    def _connect(self):
        try:
            connection = psycopg2.connect(
                database=self.db_name,
                user=self.db_user,
                password=self.db_pass,
                host=self.db_host,
                port=self.db_port)
            connection.autocommit = True

            return connection

        except OperationalError as e:
            logger.exception(f'Error while connecting to DB: {e}')

    def _sql(self, file):
        with open(os.path.join('sql', file), 'r', encoding='utf8') as f:
            return f.read()

    def execute(self, name: str, params: dict = None):
        con = cur = None

        try:
            con = self._connect()
            cur = con.cursor()

            query = self._sql(f'{name}.sql')
            cur.execute(query, params)

            return cur.fetchall()

        except Exception as e:
            if 'no results to fetch' not in str(e):
                logger.exception(f'Error while executing SQL: {e}')

        finally:
            if cur: cur.close()
            if con: con.close()
