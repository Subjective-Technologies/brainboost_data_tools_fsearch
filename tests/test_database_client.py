import pytest
from datetime import datetime, timedelta
import os
from database_client import DatabaseClientSQLite, FileMetadata

@pytest.fixture
def db_client():
    """Fixture to create a test database client"""
    # Use a test database file
    test_db = "test_myself.sqlite"
    
    # Ensure clean state
    if os.path.exists(test_db):
        os.remove(test_db)
    
    client = DatabaseClientSQLite(test_db)
    yield client
    
    # Cleanup after tests
    if os.path.exists(test_db):
        os.remove(test_db)

@pytest.fixture
def sample_file():
    """Fixture to create a sample file metadata"""
    return FileMetadata(
        id=None,
        full_path="/home/user/test.txt",
        drive="local",
        size=1024,
        modified_date=datetime(2024, 1, 1, 12, 0, 0),
        file_type="text/plain",
        custom_field="test_metadata"
    )

def test_create_file(db_client, sample_file):
    """Test creating a new file metadata entry"""
    file_id = db_client.create_file(sample_file)
    assert file_id > 0
    
    # Verify the file was created
    saved_file = db_client.read_file(file_id)
    assert saved_file is not None
    assert saved_file.full_path == sample_file.full_path
    assert saved_file.drive == sample_file.drive
    assert saved_file.size == sample_file.size
    assert saved_file.modified_date == sample_file.modified_date
    assert saved_file.file_type == sample_file.file_type
    assert saved_file.custom_field == sample_file.custom_field

def test_read_file_not_found(db_client):
    """Test reading a non-existent file"""
    file = db_client.read_file(999)
    assert file is None

def test_update_file(db_client, sample_file):
    """Test updating an existing file metadata entry"""
    # First create a file
    file_id = db_client.create_file(sample_file)
    
    # Modify the file
    updated_file = FileMetadata(
        id=file_id,
        full_path="/home/user/updated.txt",
        drive="remote",
        size=2048,
        modified_date=datetime(2024, 1, 2, 12, 0, 0),
        file_type="application/text",
        custom_field="updated_metadata"
    )
    
    # Update and verify
    success = db_client.update_file(updated_file)
    assert success
    
    # Read back and verify changes
    saved_file = db_client.read_file(file_id)
    assert saved_file is not None
    assert saved_file.full_path == updated_file.full_path
    assert saved_file.drive == updated_file.drive
    assert saved_file.size == updated_file.size
    assert saved_file.modified_date == updated_file.modified_date
    assert saved_file.file_type == updated_file.file_type
    assert saved_file.custom_field == updated_file.custom_field

def test_delete_file(db_client, sample_file):
    """Test deleting a file metadata entry"""
    # First create a file
    file_id = db_client.create_file(sample_file)
    
    # Delete and verify
    success = db_client.delete_file(file_id)
    assert success
    
    # Verify file is gone
    assert db_client.read_file(file_id) is None

def test_get_files_in_timerange(db_client):
    """Test retrieving files within a time range"""
    # Create files with different dates
    files = [
        FileMetadata(None, "/test1.txt", "local", 1024,
                    datetime(2024, 1, 1, 12, 0, 0), "text/plain", "meta1"),
        FileMetadata(None, "/test2.txt", "local", 1024,
                    datetime(2024, 1, 2, 12, 0, 0), "text/plain", "meta2"),
        FileMetadata(None, "/test3.txt", "local", 1024,
                    datetime(2024, 1, 3, 12, 0, 0), "text/plain", "meta3"),
    ]
    
    for file in files:
        db_client.create_file(file)
    
    # Test range query
    start_time = datetime(2024, 1, 1, 0, 0, 0)
    end_time = datetime(2024, 1, 2, 23, 59, 59)
    
    results = db_client.get_files_in_timerange(start_time, end_time)
    assert len(results) == 2
    assert results[0].modified_date > results[1].modified_date  # Check DESC order

def test_get_files_by_type(db_client):
    """Test retrieving files by type"""
    # Create files with different types
    files = [
        FileMetadata(None, "/test1.txt", "local", 1024,
                    datetime(2024, 1, 1, 12, 0, 0), "text/plain", "meta1"),
        FileMetadata(None, "/test2.pdf", "local", 1024,
                    datetime(2024, 1, 1, 12, 0, 0), "application/pdf", "meta2"),
        FileMetadata(None, "/test3.txt", "local", 1024,
                    datetime(2024, 1, 1, 12, 0, 0), "text/plain", "meta3"),
    ]
    
    for file in files:
        db_client.create_file(file)
    
    # Test type query
    results = db_client.get_files_by_type("text/plain")
    assert len(results) == 2
    assert all(f.file_type == "text/plain" for f in results)

def test_update_nonexistent_file(db_client, sample_file):
    """Test updating a non-existent file"""
    sample_file.id = 999  # Non-existent ID
    success = db_client.update_file(sample_file)
    assert not success

def test_delete_nonexistent_file(db_client):
    """Test deleting a non-existent file"""
    success = db_client.delete_file(999)
    assert not success

def test_custom_field_handling(db_client):
    """Test handling of custom_field"""
    # Test with custom_field
    file_with_custom = FileMetadata(
        id=None,
        full_path="/test1.txt",
        drive="local",
        size=1024,
        modified_date=datetime(2024, 1, 1, 12, 0, 0),
        file_type="text/plain",
        custom_field="test_metadata"
    )
    file_id = db_client.create_file(file_with_custom)
    saved_file = db_client.read_file(file_id)
    assert saved_file.custom_field == "test_metadata"
    
    # Test with None custom_field
    file_without_custom = FileMetadata(
        id=None,
        full_path="/test2.txt",
        drive="local",
        size=1024,
        modified_date=datetime(2024, 1, 1, 12, 0, 0),
        file_type="text/plain",
        custom_field=None
    )
    file_id = db_client.create_file(file_without_custom)
    saved_file = db_client.read_file(file_id)
    assert saved_file.custom_field is None

def test_find_optimal_time_gaps(db_client):
    """Test finding optimal time gaps between files"""
    # Create files with specific time gaps
    files = [
        # Group 1: 3 files within 5 minutes
        FileMetadata(None, "/test1.txt", "local", 1024,
                    datetime(2024, 1, 1, 12, 0, 0), "text/plain", "meta1"),
        FileMetadata(None, "/test2.txt", "local", 1024,
                    datetime(2024, 1, 1, 12, 2, 0), "text/plain", "meta2"),
        FileMetadata(None, "/test3.txt", "local", 1024,
                    datetime(2024, 1, 1, 12, 4, 0), "text/plain", "meta3"),
        
        # Gap of 2 hours
        
        # Group 2: 2 files within 5 minutes
        FileMetadata(None, "/test4.txt", "local", 1024,
                    datetime(2024, 1, 1, 14, 0, 0), "text/plain", "meta4"),
        FileMetadata(None, "/test5.txt", "local", 1024,
                    datetime(2024, 1, 1, 14, 3, 0), "text/plain", "meta5"),
        
        # Gap of 3 hours
        
        # Single file (shouldn't form a group)
        FileMetadata(None, "/test6.txt", "local", 1024,
                    datetime(2024, 1, 1, 17, 0, 0), "text/plain", "meta6"),
    ]
    
    for file in files:
        db_client.create_file(file)
    
    # Test with default parameters (min_group_size=2, max_gap_minutes=60)
    groups = db_client.find_optimal_time_gaps()
    assert len(groups) == 2  # Should find 2 groups
    
    # Verify first group (3 files)
    assert groups[0][2] == 3  # File count
    assert groups[0][0] == datetime(2024, 1, 1, 12, 0, 0)  # Start time
    assert groups[0][1] == datetime(2024, 1, 1, 12, 4, 0)  # End time
    
    # Verify second group (2 files)
    assert groups[1][2] == 2  # File count
    assert groups[1][0] == datetime(2024, 1, 1, 14, 0, 0)  # Start time
    assert groups[1][1] == datetime(2024, 1, 1, 14, 3, 0)  # End time
    
    # Test with different parameters
    groups = db_client.find_optimal_time_gaps(min_group_size=3, max_gap_minutes=30)
    assert len(groups) == 1  # Should only find the first group
    assert groups[0][2] == 3  # File count

def test_get_average_time_gap(db_client):
    """Test calculating average time gap between files"""
    # Create files with specific time gaps
    files = [
        FileMetadata(None, "/test1.txt", "local", 1024,
                    datetime(2024, 1, 1, 12, 0, 0), "text/plain", "meta1"),
        # 10 minute gap
        FileMetadata(None, "/test2.txt", "local", 1024,
                    datetime(2024, 1, 1, 12, 10, 0), "text/plain", "meta2"),
        # 20 minute gap
        FileMetadata(None, "/test3.txt", "local", 1024,
                    datetime(2024, 1, 1, 12, 30, 0), "text/plain", "meta3"),
    ]
    
    for file in files:
        db_client.create_file(file)
    
    avg_gap = db_client.get_average_time_gap()
    assert avg_gap == 15.0  # Average of 10 and 20 minutes

def test_empty_database_time_analysis(db_client):
    """Test time analysis methods with empty database"""
    groups = db_client.find_optimal_time_gaps()
    assert len(groups) == 0
    
    avg_gap = db_client.get_average_time_gap()
    assert avg_gap == 0.0

def test_get_first_and_last_timestamp(db_client):
    """Test getting the first and last timestamps from the database."""
    # Empty database
    first, last = db_client.get_first_and_last_timestamp()
    assert first is None and last is None

    # Add files with known timestamps
    files = [
        FileMetadata(None, "/a.txt", "local", 1, datetime(2024, 1, 1, 10, 0, 0), "text", "meta1"),
        FileMetadata(None, "/b.txt", "local", 1, datetime(2024, 1, 2, 12, 0, 0), "text", "meta2"),
        FileMetadata(None, "/c.txt", "local", 1, datetime(2024, 1, 3, 14, 0, 0), "text", "meta3"),
    ]
    for f in files:
        db_client.create_file(f)
    first, last = db_client.get_first_and_last_timestamp()
    assert first == datetime(2024, 1, 1, 10, 0, 0)
    assert last == datetime(2024, 1, 3, 14, 0, 0) 