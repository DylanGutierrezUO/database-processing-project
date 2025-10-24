import bplustree
import os
import tempfile

"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.table = table
        self.indices = [None] *  self.table.num_columns
        self.create_index(self.table.key) # Create index on key column by default
        

    """
    # returns the location of all records with the given value on column "column"
    """
    def locate(self, column, value) -> list[int]:
        tree = self.indices[column]
        recordLocations = tree.get(value, [])
        return recordLocations

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """
    def locate_range(self, begin, end, column):
        recordLocations = []
        tree = self.indices[column]
        for _, rid in tree.items(begin, end):
            recordLocations.append(rid)
        return recordLocations

    """
    # Create index on specific column
    """
    def create_index(self, column_number):
        # Create a dictionary to temporarily store value -> [RIDs] mapping
        value_to_rids = {}
        
        # First pass: collect all RIDs for each value
        for rid, _ in self.table.page_directory.items():
            value = self.table.read(rid, column_number)
            if value not in value_to_rids:
                value_to_rids[value] = []
            value_to_rids[value].append(rid)
        
        # Create temporary directory for indices if it doesn't exist
        temp_dir = os.path.join(tempfile.gettempdir(), 'lstore_indices')
        os.makedirs(temp_dir, exist_ok=True)
        
        # Create a unique filename for this index
        index_file = os.path.join(temp_dir, f'index_{self.table.name}_{column_number}.idx')
        
        # Create a B+-tree for the specified column
        tree = bplustree.BPlusTree(
            filename=index_file,
            key_size=8,
            value_size=1024,  # Increased to store arrays of RIDs
            order=50,
            serializer=bplustree.serializer.IntSerializer()
        )
        
        # Second pass: store arrays of RIDs in the tree
        for value, rids in value_to_rids.items():
            tree[value] = rids
            
        self.indices[column_number] = tree
        return

    """
    # Drop index of specific column
    """
    def drop_index(self, column_number):
        if self.indices[column_number] is not None:
            self.indices[column_number].close()  # Properly close the B+ tree
            self.indices[column_number] = None
        return
    
    def close_all_indices(self):
        """Close all B+ trees properly"""
        for i in range(len(self.indices)):
            if self.indices[i] is not None:
                self.indices[i].close()
                self.indices[i] = None
