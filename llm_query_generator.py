import openai
import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)

class LLMQueryGenerator:
    """
    Uses OpenAI's API to generate SQL queries based on user input and database schema.
    """
    def __init__(self, api_key: str):
        openai.api_key = api_key
        self.api_key = api_key

    def generate_sql(self, user_input: str, schema: Dict[str, Any]) -> str:
        """
        Generates an SQL query from user input.

        Args:
            user_input: The user's query description.
            schema: The database schema.
        Returns:
            A valid SQL query string.
        """
        prompt = (
            f"Given the database schema: {schema}\n"
            f"Generate ONLY the SQL query (no explanations) for: {user_input}\n"
            "Respond with nothing but the SQL query itself."
        )
        try:
            response = openai.ChatCompletion.create(
                model="gpt-4o",
                messages=[
                    {
                        "role": "system",
                        "content": "You are a MySQL query generator. Respond with ONLY the SQL query."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0.2,
                max_tokens=1500
            )
            sql_query = response.choices[0].message.content.strip()
            # Validate that the generated text resembles a SQL query
            if not any(sql_query.lower().startswith(keyword) for keyword in 
                       ['select', 'show', 'describe', 'insert', 'update', 'delete', 'create', 'alter', 'drop']):
                logger.warning("Generated SQL does not start with a valid keyword. Falling back to default.")
                return "SHOW TABLES;"
            return sql_query
        except Exception as e:
            logger.error("Error generating SQL query: %s", e)
            raise Exception(f"Failed to generate SQL query: {e}")
