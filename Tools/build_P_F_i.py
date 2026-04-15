import sqlite3
from pathlib import Path

# ==========================================
# CONFIG
# ==========================================
DB_PATH = Path("Sets/model.db")
Z_TABLE = "Z"
OUTPUT_TABLE = "P_F_i"

# ==========================================
# CONNECT
# ==========================================
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# ==========================================
# GET COLUMN NAMES
# ==========================================
cursor.execute(f"PRAGMA table_info({Z_TABLE})")
columns_info = cursor.fetchall()

all_columns = [col[1] for col in columns_info]

# Excluir columnas no binarias
excluded_cols = {"z_name", "covered_population", "covered_households"}
facility_cols = [c for c in all_columns if c not in excluded_cols]

print(f"Total instalaciones detectadas: {len(facility_cols)}")

# ==========================================
# CREATE OUTPUT TABLE
# ==========================================
cursor.execute(f"DROP TABLE IF EXISTS {OUTPUT_TABLE}")

cursor.execute(f"""
CREATE TABLE {OUTPUT_TABLE} (
    facility_name TEXT PRIMARY KEY,
    covered_population REAL,
    covered_households REAL
)
""")

# ==========================================
# EXTRACT SINGLETONS
# ==========================================
query = f"SELECT * FROM {Z_TABLE}"
rows = cursor.execute(query).fetchall()

col_index = {col: idx for idx, col in enumerate(all_columns)}

insert_data = []

for row in rows:
    # contar cuantas instalaciones activas hay
    active_facilities = [
        f for f in facility_cols if row[col_index[f]] == 1
    ]

    if len(active_facilities) == 1:
        facility = active_facilities[0]

        covered_population = row[col_index["covered_population"]]
        covered_households = row[col_index["covered_households"]]

        insert_data.append((facility, covered_population, covered_households))

# ==========================================
# INSERT DATA
# ==========================================
cursor.executemany(f"""
INSERT INTO {OUTPUT_TABLE} (
    facility_name,
    covered_population,
    covered_households
) VALUES (?, ?, ?)
""", insert_data)

conn.commit()
conn.close()

# ==========================================
# LOG
# ==========================================
print(f"Tabla '{OUTPUT_TABLE}' creada correctamente.")
print(f"Total instalaciones únicas encontradas: {len(insert_data)}")