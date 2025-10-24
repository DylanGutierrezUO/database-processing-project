# db.py
# Only implemented __init__ and create_table for M1
from lstore.table import Table
import os
import pickle

class Database():

    def __init__(self):
        self._tables_by_name = {}
        self.path = None

    def open(self, path):
        self.path = path
        os.makedirs(path, exist_ok=True)
        manifest_path = os.path.join(path, 'tables.pkl')
        if os.path.exists(manifest_path):
            with open(manifest_path, 'rb') as f:
                table_names = pickle.load(f)
            for name in table_names:
                table_file = os.path.join(path, f'table_{name}.pkl')
                if os.path.exists(table_file):
                    with open(table_file, 'rb') as tf:
                        table_data = pickle.load(tf)
                    table = Table.from_dict(table_data)
                    self._tables_by_name[name] = table

    def close(self):
        if self.path is None:
            return
        
        # First close all indices to prevent WAL file issues
        for table in self._tables_by_name.values():
            table.close()
        
        manifest_path = os.path.join(self.path, 'tables.pkl')
        with open(manifest_path, 'wb') as f:
            pickle.dump(list(self._tables_by_name.keys()), f)
            
        for name, table in self._tables_by_name.items():
            table_file = os.path.join(self.path, f'table_{name}.pkl')
            with open(table_file, 'wb') as tf:
                pickle.dump(table.to_dict(), tf)

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
    :param name: string    #Name of the table to delete
    :return: None
    """
    def drop_table(self, name):
        if name in self._tables_by_name:
            del self._tables_by_name[name]

    
    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        return self._tables_by_name.get(name, None)
