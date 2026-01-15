"""
    Page-Based storage system for the RDBMS.

    Design Decision: Why Pages?
    - Fixed-size blocks align with OS file sysetm pages (4KB)
    - Makes it easy to cache and manage memory
    - Industr standard approach (PostgreSQL, MySQL all use pages)

    A page contains:
    - Header: metadata (# of rows, free space pointer)
    - Slots: actual row data stored as serialzied dictionaries
"""

import struct
import json
from typing import List, Dict, Any, Optional

# constants
PAGE_SIZE = 4096 #4kb
HEADER_SIZE = 12


class Page:
    """
    Represents a single page of data storage

    Page Layout:
    [0-3] : Page ID (4 bytes, unsigned int)
    [4-7] : Number of rows (4 bytes, unsigned int)
    [8-11] : Free space offset (4 bytes, unsigned int)
    [12...]: Row data (serialized as JSON for simplicity)
    """

    def __init__(self, page_id: int):
        self.page_id = page_id
        self.num_rows = 0
        self.free_space_offset = HEADER_SIZE
        self.rows: List[Dict[str, Any]] = []

    def can_fit(self, row_data: Dict[str, Any]) -> bool:
        """
        Check if a row can fit in this page.
        Design Decision (DD): We serialize rows as JSON for simplicity.
        In production, you'd use a binary format for efficiency.
        """

        serialized = json.dumps(row_data).encode('utf-8')
        row_size = len(serialized) + 4 # +4 for length prefix
        available_space = PAGE_SIZE - self.free_space_offset
        return available_space >= row_size
    
    def insert_row(self, row_data: Dict[str, Any]) -> bool:
        """ 
        insert a row into this page if there's space.
        Returns True if successful, False if page is full.
        """
        if not self.can_fit(row_data):
            return False
        
        self.rows.append(row_data)
        self.num_rows += 1

        # update free space offset
        serialized = json.dumps(row_data).encode('utf-8')
        self.free_space_offset += len(serialized) + 4

        return True
    
    def get_row(self, row_index: int) -> Optional[Dict[str, Any]]:
        """Get a specific row by its index within the page."""
        if 0 <= row_index < len(self.rows):
            return self.rows[row_index]
        return None

    def update_row(self, row_index: int, new_data: Dict[str, Any]) -> bool:
        """
        Update a row at a specific index.

        Design Decision: Simple implementation - we just replace the row
        Real databases handle this more carefully to avoid fragmentation.
        """

        if 0 <= row_index < len(self.rows):
            # Check if updated row still fits
            old_size = len(json.dumps(self.rows[row_index]).encode('utf-8'))
            new_size = len(json.dumps(new_data).encode('utf-8'))

            if new_size <= old_size or self.can_fit(new_data):
                self.rows[row_index] = new_data
                # Recalculate free space offset
                self._recalculate_free_space()
                return True
        return False
    def delete_row(self, row_index: int) -> bool:
        """Delete a row at a specific index."""
        if 0 <= row_index < len(self.rows):
            self.rows.pop(row_index)
            self.num_rows -= 1
            self._recalculate_free_space()
            return True
        return False
    
    def _recalculate_free_space(self):
        """Recalculate the free space offset after modifications."""
        used_space = HEADER_SIZE
        for row in self.rows:
            serialized = json.dumps(row).encode('utf-8')
            used_space += len(serialized) + 4
        self.free_space_offset = used_space

    def serialize(self) -> bytes:
        """
        Convert page to bytes for writing to disk.

        Format:
        - Header: page_id (4B) | num_rows (4B) | free_space_offset (4B)
        - For each row: row_length (4B) | row_data (variable)
        """

        # Create header
        header = struct.pack('III', self.page_id, self.num_rows, self.free_space_offset)

        # Serialize all rows
        rows_data = b''
        for row in self.rows:
            row_json = json.dumps(row).encode('utf-8')
            row_length = len(row_json)
            rows_data += struct.pack('I', row_length)
            rows_data += row_json

        # Combine and pad to PAGE_SIZE
        page_data = header + rows_data
        padding = PAGE_SIZE - len(page_data)
        page_data += b'\x00' * padding # pad with zeros

        return page_data
    
    @staticmethod
    def deserialize(data: bytes) -> 'Page':
        """ 
        Reconstruct a Page object from bystes read from disk.
        """

        # read header
        page_id, num_rows, free_space_offset = struct.unpack('III', data[:HEADER_SIZE])

        # CREATE PAGE
        page = Page(page_id)
        page.num_rows = num_rows
        page.free_space_offset = free_space_offset

        # read rows
        offset = HEADER_SIZE
        for _ in range(num_rows):
            row_length = struct.unpack('I', data[offset:offset+4])[0]
            offset += 4
            row_json = data[offset:offset+4+row_length].decode('utf-8')
            offset += row_length
            page.rows.append(json.loads(row_json))

        return page

    def __repr__(self):
        return f"Page(id={self.page_id}, rows={self.num_rows}, free_space={PAGE_SIZE - self.free_space_offset}B)"