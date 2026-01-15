""" 
Table storage abstraction - the main interface for CRUD operations.

DD: Schema based storage
- Tables have defined schemas (clumn names adn types)
- type chekcing happens here before data is stored
- primary keys enforced at this layer
"""

from typing import Dict, List, Any, Optional, Set
from .page import Page
from .file_manager import FileManager

class Column:
    """ Represents a column definition in a table schema"""

    VALID_TYPES = {'INTEGER', 'TEXT', 'REAL', 'BOOLEAN'}

    def __init__(self, name: str, data_type: str, primary_key: bool = False,
                 unique: bool = False, nullable: bool = True):
        self.name = name
        self.data_type = data_type
        self.primary_key = primary_key
        self.unique = unique
        self.nullable = nullable if not primary_key else False

        if self.data_type not in self.VALID_TYPES:
            raise ValueError(f"Invalid data type: {data_type}.")
        
    def validate_value(self, value: Any) -> bool:
        """ Check if a value matches this column's type."""
        if value is None:
            return self.nullable
        
        if self.data_type == 'INTEGER':
            return isinstance(value, int)
        elif self.data_type == 'TEXT':
            return isinstance(value, str)
        elif self.data_type == 'REAL':
            return isinstance(value, (int, float))
        elif self.data_type == 'BOOLEAN':
            return isinstance(value, bool)
        
        return False
    
    def __repr__(self):
        flags = []
        if self.primary_key:
            flags.append("PK")
        if self.unique:
            flags.append("UNIQUE")
        if not self.nullable:
            flags.append("NOT NULL")

        flag_str = f" ({', '.join(flags)})" if flags else ""
        return f"{self.name} {self.data_type}{flag_str}"
        
class Table:
    """  
    Represents a table with schema and handles all CRUD operations.

    Responsibilities:
    - Schema validation
    - PK and Unique constraint enforcement
    - Coordinating page and file operations
    """

    def __init__(self, name: str, columns: List[Column], file_manager: FileManager):
        self.name = name
        self.columns = {col.name: col for col in columns}
        self.column_order = [col.name for col in columns]
        self.file_manger = file_manager

        # find primary key column(s)
        self.primary_key_cols = [col.name for col in columns if col.primary_key]
        self.unique_cols = [col.name for col in columns if col.unique or col.primary_key]

        # In-memory index for primary keys and unique constraints
        # DD: Simple dict-based index for prototype (real dbs use B-trees on disk)

        # track next available page ID
        self.next_page_id = 0

        # load existing data if table file exists
        if file_manager.table_exists(name):
            self._load_indexes()


    def _load_indexes(self):
        """Rebuild indexes by scanning all existing pages."""
        for page in self.file_manger.scan_all_pages(self.name):
            self.next_page_id = max(self.next_page_id, page.page_id + 1)
        
        for row_index, row in enumerate(page.rows):
            # Build primary key index
            if self.primary_key_cols:
                pk_value = self._get_primary_key_value(row)
                self.primary_key_index[pk_value] = (page.page_id, row_index)

            # Build unique indexes
            for col_name in self.unique_cols:
                if col_name in self.unique_cols:
                    self.unique_indexes[col_name].add(row[col_name])

            return True, None

    def select_all(self) -> List[Dict[str, Any]]:
        """
        Scan entire table and return all rows.

        DD: Full table scan
        - Simple to implement
        - Inefficient for large tables (would use indexes in production)
        """
        rows = []
        for page in self.file_manger.scan_all_pages(self.name):
            rows.extend(page.rows)
        return rows
    
    def select_by_primary_key(self, pk_value: Any) -> Optional[Dict[str, Any]]:
        """
        Fast lookup by primary key using index.
        This demonstrates why indexes are important
        """
        if not self.primary_key_cols:
            return None
        
        location = self.primary_key_index.get(pk_value)
        if location is None:
            return None
        
        page_id, row_index = location
        page = self.file_manger.read_page(self.name, page_id)

        if page is None:
            return None
        
        return page.get_row(row_index)
    
    def delete_by_primary_key(self, pk_value: Any) -> tuple[bool, Optional[str]]:
        """ Delete a row my primary key."""
        if not self.primary_key_cols:
            return False, "Table has no primary key"
        location = self.primary_key_index.get(pk_value)
        if location is None:
            return False, 'Row not found'
        
        page_id, row_index = location
        page = self.file_manger.read_page(self.name, page_id)

        if page is None:
            return False, "page not found"
        
        if row is None:
            return False, "Row not found in page"
        
        # Remove from page
        if not page.delete_row(row_index):
            return False, "Failed to delete row from page"
        
        # Write updated page
        if not self.file_manger.write_page(self.name, page):
            return False, 'Failed to write page'
        
        # update indexes
        del self.primary_key_index[pk_value]
        for col_name in self.unique_cols:
            if col_name in row[col_name] is not None:
                self.unique_indexes[col_name].discard(row[col_name])

        return True, None
    
    def update_by_primary_key(self, pk_value: Any, updates: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """Update specific columns of a row identified by primary key."""
        if not self.primary_key_cols:
            return False, "Table has no primary key"
        
        location = self.primary_key_index.get(pk_value)
        if location is None:
            return False, "Row not found"
        
        page_id, row_index = location
        page = self.file_manager.read_page(self.name, page_id)
        
        if page is None:
            return False, "Page not found"
        
        old_row = page.get_row(row_index)
        if old_row is None:
            return False, "Row not found in page"
        
        # Create updated row
        new_row = old_row.copy()
        new_row.update(updates)
        
        # Validate new row
        valid, error = self.validate_row(new_row)
        if not valid:
            return False, error
        
        # Update in page
        if not page.update_row(row_index, new_row):
            return False, "Failed to update row in page"
        
        # Write updated page
        if not self.file_manager.write_page(self.name, page):
            return False, "Failed to write page"
        
        # Update unique indexes (primary key can't change)
        for col_name in self.unique_cols:
            if col_name != self.primary_key_cols[0]:  # Don't update PK index
                # Remove old value
                if col_name in old_row and old_row[col_name] is not None:
                    self.unique_indexes[col_name].discard(old_row[col_name])
                # Add new value
                if col_name in new_row and new_row[col_name] is not None:
                    self.unique_indexes[col_name].add(new_row[col_name])
        
        return True, None
    
    def __repr__(self):
        cols_str = ", ".join(str(col) for col in self.columns.values())
        return f"Table('{self.name}', columns=[{cols_str}])"