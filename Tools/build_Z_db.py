import sqlite3
import pandas as pd
from itertools import product
from pathlib import Path

# -------------------------
# Paths
# -------------------------
f_db_path = Path("Sets/F.db")
z_db_path = Path("Sets/Z.db")

# -------------------------
# Load facilities
# -------------------------
conn_f = sqlite3.connect(f_db_path)

facilities_df = pd.read_sql("SELECT facility_name FROM facilities", conn_f)

conn_f.close()

facility_names = facilities_df["facility_name"].tolist()
n = len(facility_names)

print(f"Number of facilities: {n}")
print(f"Total combinations: {2**n}")

# -------------------------
# Create Z.db
# -------------------------
z_db_path.parent.mkdir(parents=True, exist_ok=True)

conn_z = sqlite3.connect(z_db_path)
cursor = conn_z.cursor()

# Drop table if exists
cursor.execute("DROP TABLE IF EXISTS z")

# Create dynamic schema
columns_sql = ",\n".join([f'"{name}" INTEGER' for name in facility_names])

create_table_sql = f"""
CREATE TABLE z (
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

for idx, combo in enumerate(product([0, 1], repeat=n)):
    z_name = f"z_{idx:06d}"
    row = (z_name, *combo)
    batch.append(row)

    if len(batch) == batch_size:
        placeholders = ",".join(["?"] * (n + 1))
        insert_sql = f"INSERT INTO z VALUES ({placeholders})"
        cursor.executemany(insert_sql, batch)
        conn_z.commit()
        batch = []
        print(f"Inserted {idx+1} rows...")

# Insert remaining
if batch:
    placeholders = ",".join(["?"] * (n + 1))
    insert_sql = f"INSERT INTO z VALUES ({placeholders})"
    cursor.executemany(insert_sql, batch)
    conn_z.commit()

conn_z.close()

print("Z.db created successfully.")