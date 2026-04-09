import sqlite3
import pandas as pd
from itertools import product
from pathlib import Path

# -------------------------
# File name: create_Z_table.py
# -------------------------

# -------------------------
# Paths
# -------------------------
db_path = Path("Sets/model.db")

# -------------------------
# Load facilities from F
# -------------------------
conn = sqlite3.connect(db_path)

facilities_df = pd.read_sql("""
    SELECT facility_name
    FROM F
    ORDER BY facility_name
""", conn)

facility_names = facilities_df["facility_name"].tolist()
n = len(facility_names)

print(f"Number of facilities: {n}")
print(f"Total combinations: {2**n}")

if n == 0:
    conn.close()
    raise ValueError("Table F is empty. Cannot create Z without facilities.")

cursor = conn.cursor()

# -------------------------
# Drop Z if exists
# -------------------------
cursor.execute('DROP TABLE IF EXISTS Z')

# -------------------------
# Create dynamic schema
# -------------------------
columns_sql = ",\n".join([f'"{name}" INTEGER NOT NULL CHECK("{name}" IN (0,1))' for name in facility_names])

create_table_sql = f"""
CREATE TABLE Z (
    z_name TEXT PRIMARY KEY,
    {columns_sql}
)
"""

cursor.execute(create_table_sql)

# -------------------------
# Generate combinations
# -------------------------
batch_size = 10000
batch = []

placeholders = ",".join(["?"] * (n + 1))
insert_sql = f"INSERT INTO Z VALUES ({placeholders})"

for idx, combo in enumerate(product([0, 1], repeat=n)):
    z_name = f"z_{idx:06d}"
    row = (z_name, *combo)
    batch.append(row)

    if len(batch) == batch_size:
        cursor.executemany(insert_sql, batch)
        conn.commit()
        batch = []
        print(f"Inserted {idx + 1} rows...")

# Insert remaining
if batch:
    cursor.executemany(insert_sql, batch)
    conn.commit()

conn.close()

print("Table Z created successfully inside Sets/model.db.")