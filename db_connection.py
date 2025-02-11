class DBConnection:
    """
    Handles the database connection using the given driver.
    """
    def __init__(self, db_driver):
        self.db_driver = db_driver

    def connect(self):
        """
        Establishes and returns a connection by calling the driver's connect method.
        """
        return self.db_driver.connect()
