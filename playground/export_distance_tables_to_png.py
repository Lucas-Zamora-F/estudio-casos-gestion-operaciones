import sqlite3
import pandas as pd
from pathlib import Path

# =============================
# CONFIG
# =============================
DB_PATH = Path("Sets/transport_matrices.db")
OUTPUT_FILE = Path("Graphs/tables/distance_tables.xlsx")

# Crear carpeta si no existe
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

# =============================
# CONNECT TO DB
# =============================
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# =============================
# GET TABLE NAMES (_distance only)
# =============================
cursor.execute("""
    SELECT name 
    FROM sqlite_master 
    WHERE type='table' AND name LIKE '%_distance'
""")

tables = [row[0] for row in cursor.fetchall()]

print(f"Found {len(tables)} distance tables:")
for t in tables:
    print(f" - {t}")

# =============================
# EXPORT TO EXCEL (MULTI-SHEET)
# =============================
with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:

    for table_name in tables:
        print(f"Processing {table_name}...")

        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)

        # Excel sheet names have max 31 chars
        sheet_name = table_name[:31]

        df.to_excel(writer, sheet_name=sheet_name, index=False)

        print(f"Added sheet -> {sheet_name}")

# =============================
# CLOSE
# =============================
conn.close()

print(f"\nExcel file saved at: {OUTPUT_FILE}")