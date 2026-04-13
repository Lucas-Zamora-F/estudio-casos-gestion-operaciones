import sqlite3
from pathlib import Path

# =============================
# CONFIG
# =============================
DB_PATH = Path("Sets/parameters.db")
BIG_M = 1e12  # very high cost to represent "not available"

# =============================
# SETS
# =============================
origins = [
    "USA",
    "Mexico",
    "Peru",
    "Bolivia",
    "Argentina",
    "Ecuador",
    "Spain"
]

products = [
    "Roma Tomatoes",
    "cauliflower",
    "broccoli",
    "asparagus",
    "Green Bell Pepper"
]

# =============================
# KNOWN DATA
# columns:
# origin, product, sea, air, land
# =============================
known_data = {
    ("Mexico", "cauliflower"): {"sea": 1.25},

    ("Peru", "Roma Tomatoes"): {"land": 0.56},
    ("Peru", "cauliflower"): {"sea": 1.05},
    ("Peru", "asparagus"): {"sea": 5.17, "air": 7.22},

    ("Ecuador", "cauliflower"): {"sea": 1.45},
    ("Ecuador", "broccoli"): {"sea": 1.23},

    ("Spain", "cauliflower"): {"sea": 1.41},
    ("Spain", "Green Bell Pepper"): {"sea": 1.18},
}

# =============================
# CONNECT
# =============================
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# =============================
# DROP AND RECREATE TABLE
# =============================
cursor.execute("DROP TABLE IF EXISTS C_pur_imp")

cursor.execute("""
CREATE TABLE C_pur_imp (
    origin TEXT,
    product TEXT,
    purchase_cost_usd_per_kg_sea REAL,
    purchase_cost_usd_per_kg_air REAL,
    purchase_cost_usd_per_kg_land REAL,
    PRIMARY KEY (origin, product)
)
""")

# =============================
# INSERT ALL COMBINATIONS
# =============================
for origin in origins:
    for product in products:
        sea_cost = BIG_M
        air_cost = BIG_M
        land_cost = BIG_M

        if (origin, product) in known_data:
            row = known_data[(origin, product)]
            sea_cost = row.get("sea", BIG_M)
            air_cost = row.get("air", BIG_M)
            land_cost = row.get("land", BIG_M)

        cursor.execute("""
            INSERT INTO C_pur_imp (
                origin,
                product,
                purchase_cost_usd_per_kg_sea,
                purchase_cost_usd_per_kg_air,
                purchase_cost_usd_per_kg_land
            )
            VALUES (?, ?, ?, ?, ?)
        """, (origin, product, sea_cost, air_cost, land_cost))

# =============================
# COMMIT & CLOSE
# =============================
conn.commit()
conn.close()

print("C_pur_imp created and filled successfully.")