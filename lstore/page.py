from lstore import config

class PageID:
    def __init__(self, table_name, column_index, page_number, is_base_page):
        self.table_name = table_name
        self.column_index = column_index
        self.page_number = page_number
        self.is_base_page = is_base_page

    def __str__(self):
        return f"{self.table_name}_{self.column_index}_{self.page_number}_{int(self.is_base_page)}"


class Page:

    def __init__(self):
        self.PageID = None
        self.num_records = 0
        self.data = [] * config.MAX_RECORDS_PER_PAGE


    def has_capacity(self):
        if self.num_records < config.MAX_RECORDS_PER_PAGE:
            return True

    def write(self, value):
        self.num_records += 1
        self.data.append(value)
        return self.num_records - 1 # Return the slot/index where the value was written
    
    def read(self, slot:int):
        if 0 <= slot < self.num_records:
            return self.data[slot], self.indirection[slot] # Return the value and indirection
        else:
            raise IndexError("Slot index out of bounds")

    def toJSON(self):
        return {
            "PageID": str(self.PageID),
            "num_records": self.num_records,
            "data": self.data
        }
    
    def fromJSON(self, json_data):
        self.PageID = PageID(*json_data["PageID"].split('_'))
        self.num_records = json_data["num_records"]
        self.data = json_data["data"]
