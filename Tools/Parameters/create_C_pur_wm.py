import sqlite3
from pathlib import Path

# =============================
# CONFIG
# =============================
DB_PATH = Path("Sets/parameters.db")

# =============================
# DATA
# =============================
data = [
    ("Roma Tomato", 0.8950),
    ("Cauliflower", 1.62),
    ("Broccoli", 2.88),
    ("Asparagus", 1.32),
    ("Green Bell Pepper", 1.10),
]

# =============================
# CREATE + INSERT
# =============================
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Crear tabla
cursor.execute("""
    CREATE TABLE IF NOT EXISTS C_pur_wm (
        product TEXT PRIMARY KEY,
        cost_usd_per_kg REAL
    )
""")

# Limpiar datos previos
cursor.execute("DELETE FROM C_pur_wm")

# Insertar datos
cursor.executemany("""
    INSERT INTO C_pur_wm (product, cost_usd_per_kg)
    VALUES (?, ?)
""", data)

conn.commit()
conn.close()

print("Tabla C_pur_wm creada y poblada correctamente.")
