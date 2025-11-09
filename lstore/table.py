import time
from . import config
from .page import Page
from .index import Index
import os
import json


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class IndirectionEntry:
    def __init__(self, pageType:int, rid):
        self.pageType = pageType # 0 for base page, 1 for tail page
        self.rid = rid 

class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns + 4 # Add 4 for metadata columns
        self.page_directory = {} # RID: (List of page locations)
        self.base_record_count = int(0) # Counter for base records
        self.tail_record_count = int(0) # Counter for tail records
        self.index = Index(self)
        self.pageBuffer = None # Will be set when table is added to database

    def link_page_buffer(self, pageBuffer):
        self.pageBuffer = pageBuffer
        return
        
    def insert_row(self, *columns):
        """
        Insert a new row into the table's base pages
        :param columns: int[]   # List of values for each user column
        :return: bool          # True if insert was successful
        """
        # Validate number of columns matches expected
        if len(columns) != self.num_columns - 4:  # Subtract metadata columns
            return False
            
        # Generate new RID for a base record
        rid = self._generate_rid("base")
        
        # Create metadata
        indirection = IndirectionEntry(0, rid)  # Points to self initially
        timestamp = int(time() * 1000)  # Current time in milliseconds
        schema_encoding = '0' * (self.num_columns - 4)  # All columns unmodified
        
        # Combine metadata and user columns
        full_record = [indirection, rid, timestamp, schema_encoding] + list(columns)
        
        # Write to base pages
        self._write_to_base_pages(rid, full_record)
        
        # Update index for every indexed column
        for col_index in range(self.num_columns - 4):
            if self.index.indices[col_index] is not None:
                self.index.insert_entry(rid, col_index, columns[col_index])
        
        return True
    
    def _generate_rid(self, page_type):
        if page_type == "base":
            return f"b{self.base_record_count}"
        else:
            return f"t{self.tail_record_count}"
        
    def _write_to_base_pages(self, rid, full_record):
        # Determine which page to write to based on current record count
        page_number = self.base_record_count // config.MAX_RECORDS_PER_PAGE
        #For each column, write to the corresponding page
        for col_index in range(self.num_columns + 4):
            page_id = f"{self.name}_{col_index}_{page_number}_1"  # Base page
            page = self.pageBuffer.get_page(page_id)  # Load page into buffer
            self.pageBuffer.pin_page(page_id)  # Pin page in buffer
            slot = page.write(full_record[col_index])  # Write user column value
            self.pageBuffer.set_dirty(page_id)  # Mark page as dirty
            self.pageBuffer.unpin_page(page_id)  # Unpin page in buffer
            if self.page_directory[rid] == None: # Initialize directory entry if it doesn't exist
                self.page_directory[rid] = [] * self.num_columns + 4
            self.page_directory[rid][col_index] = (page_id, slot)  # Update directory with RID location
        
        # Increment base record count
        self.base_record_count += 1

    def _write_to_tail_pages(self, rid, full_record):
        # Determine which page to write to based on current record count
        page_number = self.tail_record_count // config.MAX_RECORDS_PER_PAGE
        # For each column, write to the corresponding tail page
        for col_index in range(self.num_columns + 4):
            page_id = f"{self.name}_{col_index}_{page_number}_0"  # Tail page
            page = self.pageBuffer.get_page(page_id)  # Load page into buffer
            self.pageBuffer.pin_page(page_id)  # Pin page in buffer
            slot = page.write(full_record[col_index])  # Write user column value
            self.pageBuffer.set_dirty(page_id)  # Mark page as dirty
            self.pageBuffer.unpin_page(page_id)  # Unpin page in buffer
            if self.page_directory[rid] == None: # Initialize directory entry if it doesn't exist
                self.page_directory[rid] = [] * self.num_columns + 4
            self.page_directory[rid][col_index] = (page_id, slot)  # Update directory with RID location
        
        # Increment tail record count
        self.tail_record_count += 1

    def update_row(self, rid, *columns): # TODO
        """
        Update an existing row in the table's tail pages
        Use once per column changed, as this is designed to be called 
        multiple times of multiple columns are changed at once.
        :param rid: string       # RID of the record to update
        :param columns: int[]    # List of new values for each user column
        :return: bool            # True if update was successful
        """
        # Validate number of columns matches expected
        if len(columns) != self.num_columns - 4:
            return False
        
        # Check if record exists in page directory
        if rid not in self.page_directory:
            return False
        
        # Generate new RID for a tail record
        new_rid = self._generate_rid("tail")

        original_record = []
        
        for col_index in range(self.num_columns + 4):
            # Get base record location
            base_page_id, base_slot = self.page_directory[rid][col_index]
            base_page = self.pageBuffer.get_page(base_page_id)
            base_value = base_page.read(base_slot)
            original_record.append(base_value)

        # Check if any columns have been updated before from schema encoding
        originalSchema = original_record[config.SCHEMA_ENCODING_COLUMN]
        
        # Schema for this update
        schema_encoding = ''.join(['1' if columns[i] != original_record[i + 4] else '0' for i in range(self.num_columns - 4)])
        
        beenUpdatedBefore = False if '1' in schema_encoding else False # Base record has been updated before 

        if not beenUpdatedBefore:
            # Insert additional tail record with original values for unchanged columns
            firstTailRid = self._generate_rid("tail")
            first_tail_record = [IndirectionEntry(1, rid), firstTailRid, int(time() * 1000), schema_encoding] + original_record[4:]
            self._write_to_tail_pages(firstTailRid, first_tail_record)
            

        # Get latest tail page
        prev_record = []
        for col_index in range(self.num_columns + 4):
            # Get prev record location
            prev_page_id, prev_slot = self.page_directory[rid][col_index]
            prev_page = self.pageBuffer.get_page(prev_page_id)
            value = prev_page.read(prev_slot)
            prev_record.append(value)

        



        # Get latest tail record
        latest_tail_rid = original_record[config.INDIRECTION_COLUMN]
        
        timestamp = int(time() * 1000)  # Current time in milliseconds

        cumulative_columns = [columns[i] if columns[i] != original_record[i + 4] else original_record[i + 4] for i in range(self.num_columns - 4)]
        
        # Combine metadata and user columns
        full_record = [indirection, new_rid, timestamp, schema_encoding] + list(columns)
        
        # Write to tail pages
        self._write_to_tail_pages(new_rid, full_record)
        
        return True
    
    def get_page(self, page_id):
        '''
        Retrieves the specified page from disk for bufferpool.
        '''
        page_path = os.path.join(config.DATA_DIR, self.name, page_id)
        if not os.path.exists(page_path):
            return Page()  # Return an empty page if it doesn't exist
        with open(page_path, 'rb') as f:
            page_data = json.load(f)
            page = Page()
            page.fromJSON(page_data)
            return page
        
    def write_page(self, page_id, page):
        '''
        Writes the specified page to disk.
        '''
        page_path = os.path.join(config.DATA_DIR, self.name, page_id)
        os.makedirs(os.path.dirname(page_path), exist_ok=True)
        with open(page_path, 'w') as f:
            json.dump(page.toJSON(), f)


    def __merge(self):
        print("merge is happening")
        pass

    def delete(self):
        # Clean up resources, if needed
        pass
 
