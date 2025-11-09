from lstore.page import Page
from lstore import config

class pageInBuffer:
    def __init__(self, page:Page, is_dirty:bool, is_pinned:bool, last_access_time):
        self.page = page
        self.is_dirty = is_dirty
        self.is_pinned = is_pinned
        self.last_access_time = last_access_time

class Bufferpool:
    def __init__(self, db):
        self.db = db
        self.size = config.BUFFERPOOL_SIZE
        self.pages = {}  # page_id: pageInBuffer
        self.clock = 0   # For tracking access time

    def unpack_page_id(self, page_id):
        parts = page_id.split('_')
        if len(parts) != 4:
            raise ValueError("Invalid page ID format")
        
        table_name = parts[0]
        column_index = int(parts[1])
        page_number = int(parts[2])
        is_base_page = bool(int(parts[3]))
        
        return table_name, column_index, page_number, is_base_page

    def get_page(self, page_id):
        """
        Retrieves a page from the buffer pool. If not present, loads it from disk.
        Updates access time and returns the page object.
        """
        self.clock += 1
        
        if page_id in self.pages:
            page_in_buffer = self.pages[page_id]
            page_in_buffer.last_access_time = self.clock
            return page_in_buffer.page
        
        # Page not in buffer pool
        if len(self.pages) >= self.size:
            self.evict_page()
            
        page = self._load_page(page_id)
        self.pages[page_id] = pageInBuffer(
            page=page,
            is_dirty=False,
            is_pinned=False,
            last_access_time=self.clock
        )
        return page

    def _load_page(self, page_id):
        """
        Loads a page from disk into memory
        """
        table_name, column_index, page_number, is_base_page = self.unpack_page_id(page_id)
        table = self.db.get_table(table_name)
        page_id = f"{table_name}_{column_index}_{page_number}_{int(is_base_page)}"
        return table.get_page(page_id)

    def _evict_page(self):
        """
        Evicts a page using LRU policy, skipping pinned and dirty pages if possible
        """
        oldest_time = float('inf')
        victim_id = None
        
        # First try to evict clean, unpinned pages
        for page_id, page_in_buffer in self.pages.items():
            if (not page_in_buffer.is_pinned and 
                not page_in_buffer.is_dirty and 
                page_in_buffer.last_access_time < oldest_time):
                oldest_time = page_in_buffer.last_access_time
                victim_id = page_id
        
        # If no clean, unpinned pages found, try dirty unpinned pages
        if victim_id is None:
            for page_id, page_in_buffer in self.pages.items():
                if (not page_in_buffer.is_pinned and 
                    page_in_buffer.last_access_time < oldest_time):
                    oldest_time = page_in_buffer.last_access_time
                    victim_id = page_id
        
        if victim_id is None:
            raise Exception("No pages available for eviction - all pages are pinned")
        
        victim = self.pages[victim_id]
        if victim.is_dirty:
            self.write_page_to_disk(victim_id, victim.page)
            
        del self.pages[victim_id]

    def pin_page(self, page_id):
        """
        Pins a page in memory
        """
        if page_id in self.pages:
            self.pages[page_id].is_pinned = True

    def unpin_page(self, page_id):
        """
        Unpins a page in memory
        """
        if page_id in self.pages:
            self.pages[page_id].is_pinned = False

    def mark_dirty(self, page_id):
        """
        Marks a page as dirty, indicating it needs to be written back to disk
        """
        if page_id in self.pages:
            self.pages[page_id].is_dirty = True

    def write_page_to_disk(self, page_id, page):
        """
        Writes a page back to disk, assumes page is dirty
        """
        table_name, _, _, _ = self.unpack_page_id(page_id)
        table = self.db.get_table(table_name)
        table.write_page(page_id, page)

    def flush_all(self):
        """
        Flushes all dirty pages to disk
        """
        for page_id, page_in_buffer in self.pages.items():
            if page_in_buffer.is_dirty:
                self.write_page_to_disk(page_id, page_in_buffer.page)
                page_in_buffer.is_dirty = False

    def evict_all(self):
        """
        Evicts all pages from the buffer pool, flushing dirty pages to disk
        """
        while self.pages:
            self._evict_page()
