import sqlite3
from pathlib import Path

# ==========================================
# CONFIG
# ==========================================
DB_FILES = [
    Path("Sets/model.db"),
    Path("Sets/b_fz.db"),
    Path("Sets/parameters.db"),
    Path("Sets/transport_matrices.db"),
]

# ==========================================
# FUNCTION: GET TABLES
# ==========================================
def get_tables(cursor):
    cursor.execute("""
        SELECT name
        FROM sqlite_master
        WHERE type='table'
        ORDER BY name;
    """)
    return [row[0] for row in cursor.fetchall()]

# ==========================================
# FUNCTION: GET COLUMNS
# ==========================================
def get_columns(cursor, table_name):
    cursor.execute(f"PRAGMA table_info({table_name});")
    return [row[1] for row in cursor.fetchall()]

# ==========================================
# MAIN
# ==========================================
def inspect_databases():
    for db_path in DB_FILES:
        if not db_path.exists():
            print(f"⚠️ File not found: {db_path}")
            continue

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        tables = get_tables(cursor)

        if not tables:
            print(f"file: {db_path} -> (no tables found)")
        else:
            for table in tables:
                columns = get_columns(cursor, table)

                print(
                    f"file: {db_path} "
                    f"table: {table} "
                    f"Columns: {columns}"
                )

        conn.close()


# ==========================================
# RUN
# ==========================================
if __name__ == "__main__":
    inspect_databases()