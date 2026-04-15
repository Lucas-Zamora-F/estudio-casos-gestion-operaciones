import sqlite3
from pathlib import Path

# ==========================================
# CONFIG
# ==========================================
DB_PATH = Path("Sets/parameters.db")
TABLE_NAME = "product_origin_month_availability"

# ==========================================
# MASTER SETS
# ==========================================
products = [
    "Roma Tomatoes",
    "Cauliflower",
    "Broccoli",
    "Asparagus",
    "Green Bell Pepper",
]

origins = [
    "US",
    "Mexico",
    "Peru",
    "Ecuador",
    "Bolivia",
    "Argentina",
    "Spain",
    "Chile-Scl",
    "Chile-Vap",
    "Chile-Maul",
    "Chile-Coq",
]

months = {
    1: "Jan",
    2: "Feb",
    3: "Mar",
    4: "Apr",
    5: "May",
    6: "Jun",
    7: "Jul",
    8: "Aug",
    9: "Sep",
    10: "Oct",
    11: "Nov",
    12: "Dec",
}

# ==========================================
# BASE AVAILABILITY DATA
# Only combinations listed here get 1s.
# Everything else will be 0 by default.
# ==========================================
base_availability = {
    ("Roma Tomatoes", "Peru"): [6, 7, 8, 9, 10, 11],

    ("Broccoli", "Ecuador"): list(range(1, 13)),
    ("Broccoli", "Spain"): [1, 2, 3, 4, 11, 12],

    ("Asparagus", "Peru"): list(range(1, 13)),

    ("Cauliflower", "Mexico"): [6],
    ("Cauliflower", "Peru"): [9],
    ("Cauliflower", "Ecuador"): [2, 3, 9],
    ("Cauliflower", "Spain"): list(range(1, 13)),

    ("Green Bell Pepper", "Spain"): list(range(1, 13)),
}

# ==========================================
# CHILE REGIONAL AVAILABILITY
# Same seasonality for all Chile origins
# ==========================================
chile_seasonality = {
    "Roma Tomatoes": [1, 2, 3, 4, 12],
    "Broccoli": [4, 5, 6, 7, 8, 9, 10],
    "Asparagus": [9, 10, 11, 12],
    "Cauliflower": [4, 5, 6, 7, 8, 9],
    "Green Bell Pepper": [1, 2, 3, 4],
}

chile_origins = [
    "Chile-Scl",
    "Chile-Vap",
    "Chile-Maul",
    "Chile-Coq",
]

# Build final availability dictionary
availability = dict(base_availability)

for chile_origin in chile_origins:
    for product, months_available in chile_seasonality.items():
        availability[(product, chile_origin)] = months_available

# ==========================================
# CONNECT
# ==========================================
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# ==========================================
# STEP 1: CLEAN PREVIOUS TABLE
# ==========================================
print("=" * 70)
print(f"Cleaning previous table: {TABLE_NAME} (if exists)")
print("=" * 70)

cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
conn.commit()

# ==========================================
# STEP 2: CREATE TABLE
# ==========================================
columns_sql = ",\n".join([
    f"available_{month_name} INTEGER NOT NULL CHECK (available_{month_name} IN (0,1))"
    for month_name in months.values()
])

cursor.execute(f"""
    CREATE TABLE {TABLE_NAME} (
        origin TEXT NOT NULL,
        product TEXT NOT NULL,
        {columns_sql},
        PRIMARY KEY (origin, product)
    )
""")
conn.commit()

# ==========================================
# STEP 3: INSERT ALL POSSIBLE COMBINATIONS
# ==========================================
print("\nInserting all origin-product combinations...")

inserted_rows = 0

for origin in origins:
    for product in products:
        months_available = availability.get((product, origin), [])
        row = [1 if m in months_available else 0 for m in months.keys()]

        placeholders = ",".join(["?"] * (2 + 12))

        cursor.execute(f"""
            INSERT INTO {TABLE_NAME}
            VALUES ({placeholders})
        """, [origin, product] + row)

        inserted_rows += 1

conn.commit()

# ==========================================
# STEP 4: VALIDATION OUTPUT
# ==========================================
print("\n" + "=" * 70)
print(f"TABLE CREATED SUCCESSFULLY: {TABLE_NAME}")
print(f"Total rows inserted: {inserted_rows}")
print(f"Expected rows: {len(origins) * len(products)}")
print("=" * 70)

cursor.execute(f"""
    SELECT *
    FROM {TABLE_NAME}
    ORDER BY origin, product
""")

for row in cursor.fetchall():
    print(row)

conn.close()