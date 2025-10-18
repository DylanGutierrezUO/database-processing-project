# page.py
# Think of a Page as a tiny fixed-size drawer for one column.
# It's 4KB big and we drop 8-byte integers into it one after another.
# Table will own a bunch of these (a list per column) and call has_capacity()/write().

from typing import Optional

class Page:
    PAGE_SIZE_BYTES = 4096          # size of the drawer
    CELL_SIZE_BYTES = 8             # each item is a 64-bit int
    CAPACITY = PAGE_SIZE_BYTES // CELL_SIZE_BYTES  # how many ints fit (512)

    def __init__(self):
        self.num_records: int = 0          # how many we've actually stored
        self.data = bytearray(self.PAGE_SIZE_BYTES)  # the raw bytes

    def has_capacity(self) -> bool:
        # can we fit one more int?
        return self.num_records < self.CAPACITY

    def write(self, value: int) -> Optional[int]:
        """
        Append one integer to the end of this page.
        Returns the slot index (0..511), or None if the page is full.
        The Table will remember which page this value went to
        and which slot it's in so it can find it later
        """
        if not self.has_capacity():
            return None

        slot = self.num_records                 # next open slot
        start = slot * self.CELL_SIZE_BYTES     # byte offset

        # store as little-endian signed 64-bit; just keep read/write consistent
        self.data[start:start + self.CELL_SIZE_BYTES] = int(value).to_bytes(
            self.CELL_SIZE_BYTES, byteorder="little", signed=True
        )

        self.num_records += 1
        return slot

    def read(self, slot: int) -> int:
        # grab the int at a slot (Table can use this when doing selects)
        if not (0 <= slot < self.num_records):
            raise IndexError("slot out of range")
        start = slot * self.CELL_SIZE_BYTES
        return int.from_bytes(
            self.data[start:start + self.CELL_SIZE_BYTES],
            byteorder="little",
            signed=True,
        )
