import sqlite3
from pathlib import Path

# =============================
# CONFIG
# =============================
DB_PATH = Path("Sets/parameters.db")

# Precios en USD/kg
data = {
    "Roma Tomatoes": 2.42,
    "Cauliflower": 1.83,
    "Broccoli": 3.49,
    "Asparagus": 15.05,
    "Green Bell Pepper": 6.28,
}

# =============================
# CONNECT
# =============================
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# =============================
# CREATE TABLE P_k
# =============================
cursor.execute("DROP TABLE IF EXISTS P_k")

cursor.execute("""
CREATE TABLE P_k (
    product TEXT PRIMARY KEY,
    price_usd_per_kg REAL NOT NULL
)
""")

# =============================
# INSERT DATA
# =============================
for product, price in data.items():
    cursor.execute("""
        INSERT INTO P_k (product, price_usd_per_kg)
        VALUES (?, ?)
    """, (product, price))

# =============================
# COMMIT AND CLOSE
# =============================
conn.commit()
conn.close()

print("Table P_k created and filled successfully.")
print(f"Rows inserted: {len(data)}")