class SchemaFetcher:
    """
    Retrieves the schema of the database via the provided database driver.
    """
    def __init__(self, db_driver):
        self.db_driver = db_driver

    def fetch_schema(self):
        """
        Fetch and return the database schema.
        """
        return self.db_driver.fetch_schema()
