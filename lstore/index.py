"""
A data structure holding indices for various columns of a table. Key column should be indexed by default, 
other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] * (table.num_columns + 4) # Add 4 for metadata columns
        self.table = table
        # Create index for key column by default
        self.create_index(table.key)
        

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        """
        Returns the location of all records with the given value on column "column"
        """
        if self.indices[column] is None:
            return []
        
        return self.indices[column].get(value, [])

    def locate_range(self, begin, end, column):
        """
        Returns the RIDs of all records with values in column "column" between "begin" and "end"
        """
        if self.indices[column] is None:
            return []
        
        result = []
        for value in self.indices[column]:
            if begin <= value <= end:
                result.extend(self.indices[column][value])
        return result
    
    def insert_entry(self, rid, columnNum, value):
        """
        Inserts an entry into the index for the specified column
        """
        if self.indices[columnNum] is None:
            self.indices[columnNum] = {}
        
        if columnNum == self.table.key:
            self.indices[columnNum][value] = [rid]
            return

        if value not in self.indices[columnNum]:
            self.indices[columnNum][value] = []
        
        self.indices[columnNum][value].append(rid)

    def create_index(self, column_number):
        """
        # optional: Drop index of specific column
        """
        if column_number < 0 or column_number >= self.table.num_columns + 4:  # Account for metadata columns
            raise ValueError("Invalid column number")
        
        if self.indices[column_number] is not None:
            raise ValueError("Index already exists for this column")
        
        self.indices[column_number] = {}
        
        # Populate the index with existing records
        for rid, page_locations in self.table.page_directory.items():
            if rid.startswith('b'):  # Only index base records
                page_location = page_locations[column_number]
                page = self.table.pageBuffer.get_page(page_location)
                for record in page.data:
                    value = record[4 + column_number]  # User columns start at index 4
                    if value not in self.indices[column_number]:
                        self.indices[column_number][value] = []
                    self.indices[column_number][value].append(rid)

    def drop_index(self, column_number):
        """
        Drop index of specific column
        """
        if column_number < 0 or column_number >= self.table.num_columns:
            raise ValueError("Invalid column number")
            
        self.indices[column_number] = None
