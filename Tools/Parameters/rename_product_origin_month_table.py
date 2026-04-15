import sqlite3
from pathlib import Path

# ==========================================
# CONFIG
# ==========================================
DB_PATH = Path("Sets/parameters.db")
OLD_NAME = "product_origin_month_availability"
NEW_NAME = "a_ksm"

# ==========================================
# CONNECT
# ==========================================
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# ==========================================
# RENAME TABLE
# ==========================================
print(f"Renaming table '{OLD_NAME}' → '{NEW_NAME}'")

cursor.execute(f"""
    ALTER TABLE {OLD_NAME}
    RENAME TO {NEW_NAME}
""")

conn.commit()

print("Done.")

conn.close()