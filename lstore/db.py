# db.py
# Only implemented __init__ and create_table for M1
from lstore.table import Table

class Database():

    def __init__(self):
        self._tables_by_name = {}

    # Not required for milestone1
    def open(self, path):
        pass

    def close(self):
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    # Make a table and remember it by name
    def create_table(self, name: str, num_columns: int, key: int) -> Table:
        t = Table(name, num_columns, key)
        self._tables_by_name[name] = t
        return t

    
    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        pass

    
    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        pass
