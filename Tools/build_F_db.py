import sqlite3
import pandas as pd
from pathlib import Path

# -------------------------
# Paths
# -------------------------
base_path = Path("V1/SOLVER DATA")

cd_path = base_path / "CD.csv"
ds_path = base_path / "DS.csv"
mdcp_path = base_path / "MDCP.csv"

db_path = Path("Sets/F.db")  # <-- CAMBIO IMPORTANTE

# -------------------------
# Load data
# -------------------------
cd = pd.read_csv(cd_path)
ds = pd.read_csv(ds_path)
mdcp = pd.read_csv(mdcp_path)

# -------------------------
# Standardize columns
# -------------------------
cd = cd.rename(columns={"cd_name": "facility_name"})
cd["type"] = "CD"

ds = ds.rename(columns={"ds_name": "facility_name"})
ds["type"] = "DS"

mdcp = mdcp.rename(columns={"mdcp_name": "facility_name"})
mdcp["type"] = "MDCP"

# -------------------------
# Merge all
# -------------------------
df = pd.concat([cd, ds, mdcp], ignore_index=True)
df = df[["facility_name", "type", "latitude", "longitude"]]

# -------------------------
# Create DB
# -------------------------
db_path.parent.mkdir(parents=True, exist_ok=True)

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Rebuild table
cursor.execute("DROP TABLE IF EXISTS facilities")

cursor.execute("""
CREATE TABLE facilities (
    facility_name TEXT,
    type TEXT CHECK(type IN ('CD','DS','MDCP')),
    latitude REAL,
    longitude REAL
)
""")

# -------------------------
# Insert data
# -------------------------
df.to_sql("facilities", conn, if_exists="append", index=False)

conn.commit()
conn.close()

print(f"F.db created successfully at: {db_path}")