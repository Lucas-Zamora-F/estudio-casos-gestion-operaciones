import sqlite3
from pathlib import Path

# ==========================================
# CONFIG
# ==========================================
DB_PATH = Path("Sets/parameters.db")
TABLE_NAME = "M"

# ==========================================
# MONTH DATA
# ==========================================
months = [
    (1, "Jan"),
    (2, "Feb"),
    (3, "Mar"),
    (4, "Apr"),
    (5, "May"),
    (6, "Jun"),
    (7, "Jul"),
    (8, "Aug"),
    (9, "Sep"),
    (10, "Oct"),
    (11, "Nov"),
    (12, "Dec"),
]

# ==========================================
# CONNECT
# ==========================================
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# ==========================================
# CLEAN PREVIOUS TABLE
# ==========================================
print("=" * 60)
print(f"Cleaning table: {TABLE_NAME}")
print("=" * 60)

cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
conn.commit()

# ==========================================
# CREATE TABLE
# ==========================================
cursor.execute(f"""
    CREATE TABLE {TABLE_NAME} (
        month_num INTEGER PRIMARY KEY,
        month_name TEXT NOT NULL UNIQUE
    )
""")
conn.commit()

# ==========================================
# INSERT DATA
# ==========================================
print("Inserting months...")

cursor.executemany(f"""
    INSERT INTO {TABLE_NAME} (month_num, month_name)
    VALUES (?, ?)
""", months)

conn.commit()

# ==========================================
# VALIDATION OUTPUT
# ==========================================
print("\n" + "=" * 60)
print(f"TABLE CREATED: {TABLE_NAME}")
print("=" * 60)

cursor.execute(f"SELECT * FROM {TABLE_NAME}")

for row in cursor.fetchall():
    print(row)

conn.close()
