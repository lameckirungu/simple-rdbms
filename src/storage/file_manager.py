"""
File manager for handling disk I/O operations.

Design decision: One file per table
    - Simple to implement
    - Easy to debug (you can ispect individual table files)
    - Real databases use more complex storage (tablespaces, data files)

File Format:
    - Sequential pages written one after another
    - Each page is exactly PAGE_SIZE bytes
    - Page N starts at offset N * PAGE_SIZE
"""

import os
from typing import Optional
from .page import Page, PAGE_SIZE

class FileManager:
    """
    Manages reading and writing pages to disk files.
    Each table gets its own file in the data/directory.
    """

    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        # Create data dir if it doesnt exist
        os.makedirs(data_dir, exist_ok=True)

    def _get_file_path(self, table_name: str) -> str:
        """Get the file path for a given table."""
        return os.path.join(self.data_dir, f"{table_name}")
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists on disk."""
        return os.path.exists(self._get_file_path(table_name))

    def create_table_file(self, table_name: str) -> bool:
        """
        Create a new empty table file.
        Returns False if table already existsi
        """
        file_path = self._get_file_path(table_name)

        if os.path.exists(file_path):
            return False
        
        # Create new empty file
        with open(file_path, 'wb') as f:
            pass

        return True

    def delete_table_file(self, table_name: str) -> bool:
        """Delete a table file from disk."""
        file_path = self._get_file_path(table_name)

        if not os.path.exists(file_path):
            return False
        os.remove(file_path)
        return True 
    
    def write_page(self, table_name: str, page: Page) -> bool:
        """
        Write a page to disk at the appropriate offset.

        Design Decision: Direct page addressing
        - Page ID directly maps to file offset (page_id * PAGE_SIZE)
        - Fast random access
        - May waste space if pages are deleted (acceptable for prototype)
        """

        file_path = self._get_file_path(table_name)

        try:
            # calculate offset for this page
            offset = page.page_id * PAGE_SIZE

            with open(file_path, 'r+b' if os.path.exists(file_path) else 'wb') as f:
                f.seek(offset)
                f.write(page.serialize())

            return True
        except Exception as e:
            print(f"Error writing page: {e}")
            return False
        

    def read_page(self, table_name: str, page_id: int) -> Optional[Page]:
        """ 
        Read a specific page from disk.
        Returns None if page doesn't exist of file doesn't exist.
        """

        file_path = self._get_file_path(table_name)

        if not os.path.exists(file_path):
            return None
        
        try:
            offset = page_id * PAGE_SIZE

            with open(file_path, 'rb') as f:
                # check if file is large enough for this page
                f.seek(0, 2)
                file_size = f.tell()

                if offset + PAGE_SIZE > file_size:
                    return None
                
                # Read the page
                f.seek(offset)
                page_data = f.read(PAGE_SIZE)

                if len(page_data) < PAGE_SIZE:
                    return None
                
                return Page.deserialize(page_data)
            
        except Exception as e:
            print(f"Error reading page: {e}")
            return None
        
    def get_num_pages(self, table_name: str) -> int:
        """ 
        Get the total number of pages in a table file.
        useful for scanning entire tables.
        """
        file_path = self._get_file_path(table_name)

        if not os.path.exists(file_path):
            return 0
        file_size = os.path.getsize(file_path)
        return file_size // PAGE_SIZE
    
    def scan_all_pages(self, table_name: str):
        """ 
        Generator that yields all pages in a table.
        DD:
        - Memory efficient (doesn't load entire table at once)
        - Good for large tables
        - Pythonic way to handle iteration
        """
        num_pages = self.get_num_pages(table_name)

        for page_id in range(num_pages):
            page = self.read_page(table_name, page_id)

            if page and page.num_rows > 0:
                yield page

        def __repr__(self):
            return f"FileManager(data_dir='{self.data_dir}')"