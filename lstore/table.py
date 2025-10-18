# table.py
# The Table owns pages for each column and remembers where each row lives.
# Implemented: constructor, simple append-only insert, read/write a single cell,
# a key->rid lookup, and a basic sum over a key range.

from typing import Dict, List, Tuple, Optional
from lstore.index import Index
from lstore.page import Page

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:
    # just a tiny container
    def __init__(self, rid: int, key: int, columns: List[int]):
        self.rid = rid
        self.key = key
        self.columns = columns

    def __repr__(self) -> str:
        return f"Record(rid={self.rid}, key={self.key}, columns={self.columns})"


class Table:
    def __init__(self, name: str, num_columns: int, key: int):
        self.name = name
        self.key = key
        self.num_columns = num_columns

        # one Page-list per column; start with a single empty page each
        self._col_pages: List[List[Page]] = [[Page()] for _ in range(num_columns)]

        # rid bookkeeping
        self._next_rid: int = 1
        # rid -> list of (page_idx, slot) per column
        self.page_directory: Dict[int, List[Tuple[int, int]]] = {}

        # fast key lookup for M1 (very basic)
        self._key_to_rid: Dict[int, int] = {}

        # placeholder
        self.index = Index(self)

    # ---- internal: get current appendable page for a column ----
    def _current_page(self, col: int) -> Tuple[int, Page]:
        pages = self._col_pages[col]
        pid = len(pages) - 1
        page = pages[pid]
        if not page.has_capacity():
            pages.append(Page())
            pid += 1
            page = pages[pid]
        return pid, page

    # ---- INSERT whole row ----
    def insert_row(self, values: List[int]) -> Optional[int]:
        if values is None or len(values) != self.num_columns:
            return None
        pk = values[self.key]
        if pk in self._key_to_rid:           # duplicate check
            return None

        rid = self._next_rid
        self._next_rid += 1

        locs: List[Tuple[int, int]] = []
        for c, val in enumerate(values):
            pid, page = self._current_page(c)
            slot = page.write(int(val))
            if slot is None:                  # catch for none
                self._col_pages[c].append(Page())
                pid += 1
                slot = self._col_pages[c][-1].write(int(val))
                if slot is None:
                    return None
            locs.append((pid, slot))

        self.page_directory[rid] = locs
        self._key_to_rid[pk] = rid
        return rid

    # ---- READ one value ----
    def read(self, rid: int, column: int) -> Optional[int]:
        if rid not in self.page_directory or not (0 <= column < self.num_columns):
            return None
        pid, slot = self.page_directory[rid][column]
        page = self._col_pages[column][pid]
        # use page.read if present; otherwise decode from bytes
        if hasattr(page, "read"):
            return page.read(slot)  # type: ignore[attr-defined]
        start = slot * page.CELL_SIZE_BYTES  # type: ignore[attr-defined]
        return int.from_bytes(page.data[start:start + page.CELL_SIZE_BYTES],  # type: ignore[attr-defined]
                              byteorder="little", signed=True)

    # ---- WRITE one value (very basic) ----
    def write(self, rid: int, column: int, value: int) -> bool:
        if rid not in self.page_directory or not (0 <= column < self.num_columns):
            return False
        pid, slot = self.page_directory[rid][column]
        page = self._col_pages[column][pid]
        try:
            start = slot * page.CELL_SIZE_BYTES  # type: ignore[attr-defined]
            page.data[start:start + page.CELL_SIZE_BYTES] = int(value).to_bytes(  # type: ignore[attr-defined]
                page.CELL_SIZE_BYTES, byteorder="little", signed=True)  # type: ignore[attr-defined]
            return True
        except Exception:
            return False

    # ---- DELETE by primary key (very basic) ----
    def delete_by_key(self, key_value: int) -> bool:
        rid = self._key_to_rid.pop(key_value, None)
        if rid is None:
            return False
        self.page_directory.pop(rid, None)
        return True

    # ---- helpers used by Query ----
    def rid_for_key(self, key_value: int) -> Optional[int]:
        return self._key_to_rid.get(key_value)

    def sum_range(self, start_key: int, end_key: int, column: int) -> int:
        if not (0 <= column < self.num_columns):
            return 0
        total = 0
        matched = False
        for k, rid in self._key_to_rid.items():
            if start_key <= k <= end_key:
                matched = True
                v = self.read(rid, column)
                total += 0 if v is None else v
        return total if matched else 0

    # merge placeholder (not used in M1)
    def __merge(self):
        pass
