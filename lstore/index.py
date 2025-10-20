import bplustree
"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns
        self.create_index(table.key) # Create index on key column by default
        pass

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        pass

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        pass

    """
    # Create index on specific column
    """

    def create_index(self, column_number):
            # Create a B+-tree for the specified column
            # Key size is set to 8 bytes (for integers), value size is 8 bytes (for RIDs)
            tree = bplustree.BPlusTree(
                filename=None,  # In-memory tree
                key_size=8,
                value_size=8,
                order=50,
                serializer=bplustree.serializer.IntSerializer()
            )
            # Populate the tree with existing records
            for rid, record in enumerate(self.table.records):
                value = record[column_number]
                tree[value] = rid
            self.indices[column_number] = tree

    """
    # Drop index of specific column
    """

    def drop_index(self, column_number):
        pass
