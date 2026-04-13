import sqlite3
from pathlib import Path

# =============================
# CONFIG
# =============================
DB_PATH = Path("Sets/parameters.db")

# =============================
# DATA
# monthly per-capita demand in kg
# converted to annual per-capita demand in kg
# =============================
monthly_data = [
    ("Roma Tomatoes", 1.4583),
    ("Cauliflower", 0.2480),
    ("Broccoli", 0.1260),
    ("Asparagus", 0.0120),
    ("Green Bell Pepper", 0.1058),
]

annual_data = [(product, monthly_value * 12) for product, monthly_value in monthly_data]

# =============================
# CONNECT
# =============================
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# =============================
# DROP AND CREATE TABLE
# =============================
cursor.execute("DROP TABLE IF EXISTS D_pc_ann")

cursor.execute("""
CREATE TABLE D_pc_ann (
    product TEXT PRIMARY KEY,
    annual_per_capita_demand_kg REAL
)
""")

# =============================
# INSERT DATA
# =============================
cursor.executemany("""
    INSERT INTO D_pc_ann (
        product,
        annual_per_capita_demand_kg
    )
    VALUES (?, ?)
""", annual_data)

# =============================
# COMMIT & CLOSE
# =============================
conn.commit()
conn.close()

print("D_pc_ann created and filled successfully.")