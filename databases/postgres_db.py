import psycopg2
from psycopg2 import OperationalError, DatabaseError, extensions
from typing import Dict, List, Optional, Union
from .base_db import BaseDB
import logging

logger = logging.getLogger(__name__)

class PostgresDB(BaseDB):
    """
    PostgreSQL Database driver that provides connection management,
    schema fetching, and robust query execution.
    """
    def __init__(self, host: str, port: int, dbname: str, user: str, password: str, 
                 connect_timeout: int = 5):
        self.connection_params = {
            'host': host,
            'port': port,
            'dbname': dbname,
            'user': user,
            'password': password,
            'connect_timeout': connect_timeout
        }
        self.conn: Optional[extensions.connection] = None

    def connect(self) -> extensions.connection:
        """
        Establishes a connection to the PostgreSQL database.
        """
        try:
            self.conn = psycopg2.connect(**self.connection_params)
            logger.info("PostgreSQL connection established successfully.")
            return self.conn
        except OperationalError as err:
            logger.error("Failed to connect to PostgreSQL: %s", err)
            raise Exception(f"Connection failed: {err}")

    def fetch_schema(self) -> Dict[str, List[str]]:
        """
        Retrieves the public schema by fetching tables and their corresponding columns.
        """
        if self.conn is None or self.conn.closed:
            self.connect()
        schema: Dict[str, List[str]] = {}
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
                )
                tables = cursor.fetchall()
                for table in tables:
                    table_name = table[0]
                    cursor.execute(
                        "SELECT column_name FROM information_schema.columns WHERE table_name = %s;", 
                        (table_name,)
                    )
                    columns = cursor.fetchall()
                    schema[table_name] = [col[0] for col in columns]
            logger.info("Schema fetched successfully.")
            return schema
        except DatabaseError as err:
            logger.error("Error fetching schema: %s", err)
            raise Exception(f"Error fetching schema: {err}")

    def execute_query(self, query: str) -> Union[List, None]:
        """
        Execute one or multiple SQL queries.

        For queries returning result sets, returns the results;
        otherwise, commits the transaction.
        """
        if self.conn is None or self.conn.closed:
            self.connect()
        results = []
        # Split on semicolons, but ignore empty statements.
        queries = [q.strip() for q in query.split(';') if q.strip()]
        try:
            with self.conn.cursor() as cursor:
                for single_query in queries:
                    try:
                        cursor.execute(single_query)
                        if cursor.description:
                            results.append(cursor.fetchall())
                        else:
                            self.conn.commit()
                    except DatabaseError as q_err:
                        logger.error("Error executing query '%s': %s", single_query, q_err)
                        self.conn.rollback()
                        raise Exception(f"Error executing query: {q_err}")
            return results[0] if len(results) == 1 else results
        except DatabaseError as err:
            logger.error("General error during query execution: %s", err)
            raise Exception(f"Query execution failed: {err}")
