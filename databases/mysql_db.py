import mysql.connector
from mysql.connector import pooling, Error, MySQLConnection
from .base_db import BaseDB
from typing import Optional, Dict, List, Union
import logging

logger = logging.getLogger(__name__)

class MySQLDB(BaseDB):
    """
    MySQL Database driver that provides connection pooling, schema fetching,
    and robust query execution.
    """
    def __init__(self, host: str, port: int, database: str, user: str, password: str, ssl_required: bool = False):
        self.connection_params = {
            'host': host,
            'port': port,
            'database': database,
            'user': user,
            'password': password
        }
        if ssl_required:
            self.connection_params.update({
                'ssl_disabled': False,
                'ssl_verify_identity': False,
                'ssl_verify_cert': False
            })
        
        try:
            # Create a connection pool with pool_size=5 (adjustable as needed)
            self.pool = pooling.MySQLConnectionPool(
                pool_name="cognidb_pool",
                pool_size=5,
                **self.connection_params
            )
            self.conn: Optional[MySQLConnection] = None
            logger.info("MySQL connection pool created successfully.")
        except Error as err:
            logger.error("Error creating connection pool: %s", err)
            raise Exception(f"Failed to create connection pool: {err}")

    def connect(self) -> MySQLConnection:
        """
        Acquire a connection from the pool.
        """
        try:
            self.conn = self.pool.get_connection()
            logger.info("Successfully acquired a connection from the pool.")
            return self.conn
        except Error as err:
            logger.error("Failed to connect to MySQL: %s", err)
            raise Exception(f"Failed to connect to MySQL: {err}")

    def fetch_schema(self) -> Dict[str, List[str]]:
        """
        Retrieves the database schema by fetching tables and their corresponding columns.
        """
        if self.conn is None or not self.conn.is_connected():
            self.connect()
        schema: Dict[str, List[str]] = {}
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("SHOW TABLES;")
                tables = cursor.fetchall()
                for table in tables:
                    table_name = table[0]
                    cursor.execute(f"DESCRIBE {table_name};")
                    columns = cursor.fetchall()
                    schema[table_name] = [col[0] for col in columns]
            logger.info("Schema fetched successfully.")
            return schema
        except Error as err:
            logger.error("Error fetching schema: %s", err)
            raise Exception(f"Error fetching schema: {err}")

    def execute_query(self, query: str) -> Union[List, None]:
        """
        Execute one or multiple SQL queries.
        
        Splits queries by semicolon, executes them and returns the result (for select-type queries).
        """
        if self.conn is None or not self.conn.is_connected():
            self.connect()

        results = []
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
                    except Error as q_err:
                        logger.error("Error executing query '%s': %s", single_query, q_err)
                        raise Exception(f"Error executing query: {q_err}")
            # Return a single result if only one query was executed, otherwise all results.
            return results[0] if len(results) == 1 else results
        except Error as err:
            logger.error("General error during query execution: %s", err)
            raise Exception(f"Query execution failed: {err}")
