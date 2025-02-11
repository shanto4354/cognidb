from typing import Any

class QueryExecutor:
    """
    Executes SQL queries using a provided database driver.
    """
    def __init__(self, db_driver: Any):
        self.db_driver = db_driver

    def execute_sql(self, sql_query: str) -> Any:
        """
        Executes the given SQL query using the driver.

        Args:
            sql_query: The SQL query to execute.
        Returns:
            The result of the query execution.
        """
        return self.db_driver.execute_query(sql_query)
