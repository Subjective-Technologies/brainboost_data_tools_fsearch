import sqlite3
from datetime import datetime
from dataclasses import dataclass
from typing import Optional, List
import logging
import os

@dataclass
class FileMetadata:
    """Data class representing a file's metadata"""
    id: Optional[int]
    full_path: str
    drive: str
    size: int
    modified_date: datetime
    file_type: str
    custom_field: Optional[str] = None

    @classmethod
    def from_row(cls, row: tuple) -> 'FileMetadata':
        """Create a FileMetadata instance from a database row"""
        id_, full_path, drive, size, modified_date, file_type, custom_field = row
        return cls(
            id=id_,
            full_path=full_path,
            drive=drive,
            size=size,
            modified_date=datetime.strptime(modified_date, "%Y-%m-%d %H:%M:%S"),
            file_type=file_type,
            custom_field=custom_field
        )

class DatabaseClientSQLite:
    """SQLite client for managing file metadata"""
    
    def __init__(self, db_path: str = "myself.sqlite"):
        """Initialize database connection"""
        self.db_path = db_path
        self._ensure_db_exists()
    
    def _ensure_db_exists(self):
        """Ensure database and schema exist"""
        create_db = not os.path.exists(self.db_path)
        
        with sqlite3.connect(self.db_path) as conn:
            if create_db:
                logging.info(f"Creating new database at {self.db_path}")
                cursor = conn.cursor()
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS files (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        full_path TEXT NOT NULL,
                        drive TEXT NOT NULL,
                        size INTEGER NOT NULL,
                        modified_date TEXT NOT NULL,
                        file_type TEXT,
                        custom_field TEXT
                    )
                """)
                # Create indexes for better performance
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_modified_date ON files(modified_date)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_full_path ON files(full_path)")
                conn.commit()
    
    def create_file(self, file: FileMetadata) -> int:
        """Create a new file metadata entry"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO files (full_path, drive, size, modified_date, file_type, custom_field)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                file.full_path,
                file.drive,
                file.size,
                file.modified_date.strftime("%Y-%m-%d %H:%M:%S"),
                file.file_type,
                file.custom_field
            ))
            conn.commit()
            return cursor.lastrowid
    
    def read_file(self, file_id: int) -> Optional[FileMetadata]:
        """Read a file metadata entry by ID"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM files WHERE id = ?", (file_id,))
            row = cursor.fetchone()
            return FileMetadata.from_row(row) if row else None
    
    def update_file(self, file: FileMetadata) -> bool:
        """Update an existing file metadata entry"""
        if file.id is None:
            raise ValueError("File ID must be provided for update")
            
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE files 
                SET full_path = ?, drive = ?, size = ?, modified_date = ?, file_type = ?, custom_field = ?
                WHERE id = ?
            """, (
                file.full_path,
                file.drive,
                file.size,
                file.modified_date.strftime("%Y-%m-%d %H:%M:%S"),
                file.file_type,
                file.custom_field,
                file.id
            ))
            conn.commit()
            return cursor.rowcount > 0
    
    def delete_file(self, file_id: int) -> bool:
        """Delete a file metadata entry"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM files WHERE id = ?", (file_id,))
            conn.commit()
            return cursor.rowcount > 0
    
    def get_files_in_timerange(self, start_time: datetime, end_time: datetime) -> List[FileMetadata]:
        """Get all files modified between start_time and end_time"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM files 
                WHERE modified_date BETWEEN ? AND ?
                ORDER BY modified_date DESC
            """, (
                start_time.strftime("%Y-%m-%d %H:%M:%S"),
                end_time.strftime("%Y-%m-%d %H:%M:%S")
            ))
            return [FileMetadata.from_row(row) for row in cursor.fetchall()]
    
    def get_files_by_type(self, file_type: str) -> List[FileMetadata]:
        """Get all files of a specific type"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM files WHERE file_type = ?", (file_type,))
            return [FileMetadata.from_row(row) for row in cursor.fetchall()]
            
    def find_optimal_time_gaps(self, min_group_size: int = 2, max_gap_minutes: int = 60) -> List[tuple[datetime, datetime, int]]:
        """Find optimal time gaps for grouping files based on their modification times.
        
        This method analyzes the temporal distribution of files and finds natural groupings
        by looking at the time gaps between consecutive files.
        
        Args:
            min_group_size: Minimum number of files that should be in a group
            max_gap_minutes: Maximum gap in minutes to consider for grouping
            
        Returns:
            List of tuples containing (start_time, end_time, file_count) for each group
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # First, get all timestamps ordered by time
            cursor.execute("""
                SELECT modified_date 
                FROM files 
                WHERE modified_date IS NOT NULL 
                AND modified_date LIKE '____-__-__ __:__:__'
                ORDER BY modified_date ASC
            """)
            
            timestamps = []
            current_group_start = None
            current_group_count = 0
            groups = []
            
            # Convert all timestamps to datetime objects
            for (date_str,) in cursor.fetchall():
                try:
                    dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
                    timestamps.append(dt)
                except ValueError:
                    continue
            
            if not timestamps:
                return []
                
            # Initialize the first group
            current_group_start = timestamps[0]
            current_group_count = 1
            group_start_idx = 0
            
            # Analyze gaps between consecutive timestamps
            for i in range(1, len(timestamps)):
                time_diff = (timestamps[i] - timestamps[i-1]).total_seconds() / 60  # Convert to minutes
                
                # If the gap is too large or we're at the end, close the current group
                if time_diff > max_gap_minutes or i == len(timestamps) - 1:
                    # If the current group has enough files, add it to the results
                    if current_group_count >= min_group_size:
                        groups.append((
                            timestamps[group_start_idx],  # start time
                            timestamps[i-1],              # end time
                            current_group_count           # file count
                        ))
                    
                    # Start a new group
                    current_group_start = timestamps[i]
                    current_group_count = 1
                    group_start_idx = i
                else:
                    # Continue the current group
                    current_group_count += 1
            
            # Handle the last group if it meets the minimum size
            if current_group_count >= min_group_size:
                groups.append((
                    timestamps[group_start_idx],
                    timestamps[-1],
                    current_group_count
                ))
            
            return groups
            
    def get_average_time_gap(self) -> float:
        """Calculate the average time gap between consecutive files in minutes."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Get all timestamps ordered by time
            cursor.execute("""
                SELECT modified_date 
                FROM files 
                WHERE modified_date IS NOT NULL 
                AND modified_date LIKE '____-__-__ __:__:__'
                ORDER BY modified_date ASC
            """)
            
            total_gap = 0
            count = 0
            last_time = None
            
            for (date_str,) in cursor.fetchall():
                try:
                    current_time = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
                    if last_time is not None:
                        gap_minutes = (current_time - last_time).total_seconds() / 60
                        total_gap += gap_minutes
                        count += 1
                    last_time = current_time
                except ValueError:
                    continue
            
            return total_gap / count if count > 0 else 0.0 

    def get_first_and_last_timestamp(self) -> tuple[datetime, datetime]:
        """Return the earliest and latest modified_date in the database as (first, last)."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT MIN(modified_date), MAX(modified_date)
                FROM files
                WHERE modified_date IS NOT NULL
                AND modified_date LIKE '____-__-__ __:__:__'
            ''')
            min_date_str, max_date_str = cursor.fetchone()
            if min_date_str and max_date_str:
                return (
                    datetime.strptime(min_date_str, "%Y-%m-%d %H:%M:%S"),
                    datetime.strptime(max_date_str, "%Y-%m-%d %H:%M:%S")
                )
            return (None, None) 