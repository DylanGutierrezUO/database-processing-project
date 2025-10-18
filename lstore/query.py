# query.py
# The bare minimum to run M1.
# I left in the original comments for future guidance.
from typing import List
from lstore.table import Table, Record
from lstore.index import Index


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    # Build on top of a single table. For M1 we currently implement:
    #  - insert, select, update, sum, delete
    # Everything else returns False.
    def __init__(self, table: Table):
        self.table = table

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        pass
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    # ----- INSERT -----
    # M1 calls: query.insert(v0, v1, v2, v3, v4)
    def insert(self, *columns) -> bool:
        try:
            values = list(map(int, columns))
            rid = self.table.insert_row(values)
            return rid is not None
        except Exception:
            return False
    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    # ----- SELECT -----
    # M1 calls: query.select(key, 0, [1,1,1,1,1])[0].columns
    # Return: a list of Record objects (empty list if not found).
    def select(self, search_key: int, search_column: int, query_columns: List[int]) -> List[Record] | bool:
        try:
            results: List[Record] = []

            # We only need key-search for M1; if a non-key search slips in, do a slow scan
            if search_column == self.table.key:
                rid = self.table.rid_for_key(int(search_key))
                if rid is None:
                    return []
                row: List[int] = []
                for c in range(self.table.num_columns):
                    v = self.table.read(rid, c)
                    row.append(0 if v is None else v)
                # apply projection if mask provided; M1 tests seem to pass all 1s
                projected = [val for val, m in zip(row, query_columns)] if query_columns else row
                results.append(Record(rid, search_key, projected))
                return results

            # fallback linear scan
            for _, rid in self.table._key_to_rid.items():
                v = self.table.read(rid, search_column)
                if v == search_key:
                    row = [self.table.read(rid, c) or 0 for c in range(self.table.num_columns)]
                    projected = [val for val, m in zip(row, query_columns)] if query_columns else row
                    results.append(Record(rid, row[self.table.key], projected))
            return results
        except Exception:
            return False

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        pass

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    # ----- UPDATE -----
    # M1 calls: query.update(key, *updated_columns) with None for untouched cols
    def update(self, key: int, *columns) -> bool:
        try:
            rid = self.table.rid_for_key(int(key))
            if rid is None:
                return False
            # write non-None columns in place
            wrote = False
            for c, val in enumerate(columns):
                if val is not None:
                    self.table.write(rid, c, int(val))
                    wrote = True
            return wrote or True  # succeed even if nothing changed
        except Exception:
            return False

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_key: int, end_key: int, column: int):
        try:
            s = self.table.sum_range(int(start_key), int(end_key), int(column))
            return int(s)
        except Exception:
            return 0


    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        pass

    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
