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

        # TODO: Only if not already present
        self.create_database(self.connection(), self.sql('create_db.sql'))

    def connection(self):
        connection = None
        try:
            connection = psycopg2.connect(
                database=self.db_name,
                user=self.db_user,
                password=self.db_pass,
                host=self.db_host,
                port=self.db_port,
            )
            logger.debug('Connection to PostgreSQL DB successful')
        except OperationalError as e:
            logger.exception(f"The error '{e}' occurred")
        return connection

    def sql(self, file):
        with open(file, "r", encoding="utf8") as f:
            return f.read()

    def create_database(self, connection, query):
        connection.autocommit = True
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            logger.debug('Query executed successfully')
        except OperationalError as e:
            logger.exception(f"The error '{e}' occurred")
