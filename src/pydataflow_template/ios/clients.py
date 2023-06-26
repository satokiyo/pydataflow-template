from contextlib import contextmanager
from typing import Dict

import mysql.connector
from mysql.connector.errors import Error as MySQLConnectorError
from psycopg2 import Error as PostgresConnectorError
import psycopg2.extras


class PostgresClient:
    @contextmanager
    def get_cursor(self, config: Dict):
        """A wrapper object to connect postgres."""
        try:
            conn = psycopg2.connect(**config)
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            yield cur
        except PostgresConnectorError as e:
            raise PostgresConnectorError(f"Failed to connect postgres, Raise exception: {e}")
        finally:
            cur.close()
            conn.close()


class MysqlClient:
    @contextmanager
    def get_cursor(self, config: Dict):
        """A wrapper object to connect mysql."""
        try:
            conn = mysql.connector.connect(**config)
            cur = conn.cursor(dictionary=True, buffered=True)
            yield cur
        except MySQLConnectorError as e:
            raise MySQLConnectorError(f"Failed to connect mysql, Raise exception: {e}")
        finally:
            cur.close()
            conn.close()
