import sqlite3
from pathlib import Path

# =============================
# CONFIG
# =============================
DB_PATH = Path("Sets/parameters.db")

# =============================
# DATA
# =============================
products = [
    "Roma Tomato",
    "Cauliflower",
    "Broccoli",
    "Asparagus",
    "Green Bell Pepper",
]

origins = [
    "Chile-Scl",
    "Chile-Vap",
    "Chile-Maul",
    "Chile-Coq",
]

# Todos los orígenes tienen los mismos costos
costs = {
    "Roma Tomato": 0.6265,
    "Cauliflower": 1.1355,
    "Broccoli": 2.0160,
    "Asparagus": 0.9240,
    "Green Bell Pepper": 0.7700,
}

# Construcción de data (cartesiano)
data = []
for origin in origins:
    for product in products:
        data.append((origin, product, costs[product]))

# =============================
# CREATE + INSERT
# =============================
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Crear tabla
cursor.execute("""
    CREATE TABLE IF NOT EXISTS C_pur_cl (
        origin TEXT,
        product TEXT,
        cost_usd_per_kg REAL,
        PRIMARY KEY (origin, product)
    )
""")

# Limpiar datos previos
cursor.execute("DELETE FROM C_pur_cl")

# Insertar datos
cursor.executemany("""
    INSERT INTO C_pur_cl (origin, product, cost_usd_per_kg)
    VALUES (?, ?, ?)
""", data)

conn.commit()
conn.close()

print("Tabla C_pur_cl creada y poblada correctamente.")