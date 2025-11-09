from .table import Table
from .pagebuffer import Bufferpool

class Database():

    def __init__(self):
        self.tables = []
        self.bufferpool = Bufferpool(self)

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
    def create_table(self, name, num_columns, key_index):
        table = Table(name, num_columns, key_index)
        table.link_page_buffer(self.bufferpool)
        return table

    
    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        for i, table in enumerate(self.tables):
            if table.name == name:
                self.tables[i].delete() # Call delete method to clean up resources
                del self.tables[i]
                return
        raise ValueError("Table not found")

    
    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        for table in self.tables:
            if table.name == name:
                return table
        raise ValueError("Table not found")
