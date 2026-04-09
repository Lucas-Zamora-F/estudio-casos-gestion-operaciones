import sqlite3
from pathlib import Path

# =============================
# CONFIG
# =============================
DB_PATH = Path("Sets/model.db")

# Crear carpeta si no existe
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

# =============================
# CREATE EMPTY DB
# =============================
conn = sqlite3.connect(DB_PATH)
conn.close()

print(f"Empty database created at: {DB_PATH}")
