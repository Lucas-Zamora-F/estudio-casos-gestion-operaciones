import sqlite3
from pathlib import Path

# =============================
# CONFIG
# =============================
DB_PATH = Path("Sets/parameters.db")
MAX_ROWS = 20  # límite por tabla

# =============================
# CONNECT
# =============================
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# =============================
# GET TABLE NAMES
# =============================
cursor.execute("""
SELECT name 
FROM sqlite_master 
WHERE type='table'
ORDER BY name
""")

tables = [row[0] for row in cursor.fetchall()]

print("\n=============================")
print("TABLES IN parameters.db")
print("=============================")

for table in tables:
    print(f"\n--- TABLE: {table} ---")

    # =============================
    # GET COLUMN NAMES
    # =============================
    cursor.execute(f"PRAGMA table_info({table})")
    columns = [col[1] for col in cursor.fetchall()]
    print("Columns:", columns)

    # =============================
    # GET ROW COUNT
    # =============================
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    total_rows = cursor.fetchone()[0]
    print(f"Total rows: {total_rows}")

    # =============================
    # FETCH SAMPLE ROWS
    # =============================
    cursor.execute(f"SELECT * FROM {table} LIMIT {MAX_ROWS}")
    rows = cursor.fetchall()

    print(f"Showing first {len(rows)} rows:")

    for r in rows:
        print(r)

# =============================
# CLOSE
# =============================
conn.close()

print("\nDone.")