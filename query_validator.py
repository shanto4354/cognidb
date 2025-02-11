import sqlparse
import logging
from typing import Union

logger = logging.getLogger(__name__)

class QueryValidator:
    def validate_sql(self, sql_query: str) -> Union[bool, str]:
        """
        Validate an SQL query and attempt auto-correction if needed.

        Returns:
            True if valid, or a corrected query string if a correction was applied.
        Raises:
            ValueError: If the SQL remains invalid after attempted correction.
        """
        try:
            parsed = sqlparse.parse(sql_query)
            if not parsed:
                raise ValueError("Parsed query is empty.")
            # Additional validation logic can be added here.
            return True
        except Exception as e:
            corrected_query = self.attempt_auto_correction(sql_query)
            if corrected_query:
                logger.info("SQL auto-corrected from '%s' to '%s'", sql_query, corrected_query)
                return corrected_query
            logger.error("SQL validation error: %s", e)
            raise ValueError(f"SQL validation error: {e}")

    def attempt_auto_correction(self, sql_query: str) -> Union[str, None]:
        """
        Try to auto-correct common syntax issues in the SQL query.

        Returns:
            A corrected query string if changes were applied, else None.
        """
        corrected_query = sql_query.strip()
        if not corrected_query.endswith(";"):
            corrected_query += ";"
        if corrected_query != sql_query:
            return corrected_query
        return None
