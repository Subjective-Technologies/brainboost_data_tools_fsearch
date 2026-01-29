import sqlite3
import os

# Paths
SRC_DB = "/brainboost/brainboost_data/data_source/brainboost_data_source_rclone/search_rclone_index_db.sqlite"
DST_DB = "myself.sqlite"

# Table schema with an extra custom field
CREATE_TABLE_SQL = '''
CREATE TABLE files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    full_path TEXT NOT NULL,
    drive TEXT NOT NULL,
    size INTEGER NOT NULL,
    modified_date TEXT,
    file_type TEXT,
    custom_field TEXT
);
'''

# Remove destination if it exists
if os.path.exists(DST_DB):
    os.remove(DST_DB)

# Connect to source and destination databases
src_conn = sqlite3.connect(SRC_DB)
src_cur = src_conn.cursor()
dst_conn = sqlite3.connect(DST_DB)
dst_cur = dst_conn.cursor()

# Create the new table
print(f"Creating table in {DST_DB} ...")
dst_cur.execute(CREATE_TABLE_SQL)

def copy_and_swap():
    print(f"Copying records from {SRC_DB} to {DST_DB}, swapping modified_date and file_type ...")
    src_cur.execute("SELECT id, full_path, drive, size, modified_date, file_type FROM files")
    count = 0
    for row in src_cur.fetchall():
        # Swap modified_date and file_type, custom_field is empty
        id_, full_path, drive, size, modified_date, file_type = row
        dst_cur.execute(
            "INSERT INTO files (id, full_path, drive, size, modified_date, file_type, custom_field) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (id_, full_path, drive, size, modified_date, file_type,"")
        )
        count += 1
        if count % 100000 == 0:
            print(f"Processed {count} records...")
    dst_conn.commit()
    print(f"Done. Total records copied: {count}")

if __name__ == "__main__":
    copy_and_swap()
    src_conn.close()
    dst_conn.close()
    print(f"Output written to {DST_DB}") 