import sqlite3
from pathlib import Path

# =============================
# CONFIG
# =============================
MODEL_DB_PATH = Path("Sets/model.db")
OUTPUT_DB_PATH = Path("Sets/b_fz.db")  # NUEVO ARCHIVO

EXCLUDED_COLUMNS = {
    "z_name",
    "covered_population",
    "covered_households"
}

# =============================
# CONNECT
# =============================
model_conn = sqlite3.connect(MODEL_DB_PATH)
model_cursor = model_conn.cursor()

# Crear (o abrir) nuevo .db
output_conn = sqlite3.connect(OUTPUT_DB_PATH)
output_cursor = output_conn.cursor()

# =============================
# GET Z TABLE COLUMNS
# =============================
model_cursor.execute("PRAGMA table_info(Z)")
z_table_info = model_cursor.fetchall()

z_columns = [col[1] for col in z_table_info]

if "z_name" not in z_columns:
    raise ValueError("The Z table must contain a 'z_name' column.")

facility_columns = [col for col in z_columns if col not in EXCLUDED_COLUMNS]

if not facility_columns:
    raise ValueError("No facility columns found in table Z after excluding metadata columns.")

# =============================
# CREATE b_fz TABLE IN NEW DB
# =============================
output_cursor.execute("DROP TABLE IF EXISTS b_fz")

output_cursor.execute("""
CREATE TABLE b_fz (
    facility TEXT,
    z_name TEXT,
    is_open INTEGER,
    PRIMARY KEY (facility, z_name)
)
""")

# =============================
# READ ALL CONFIGURATIONS
# =============================
quoted_columns = ", ".join([f'"{c}"' for c in ["z_name"] + facility_columns])
select_query = f"SELECT {quoted_columns} FROM Z"

model_cursor.execute(select_query)
rows = model_cursor.fetchall()

# =============================
# FILL b_fz
# =============================
for row in rows:
    z_name = row[0]

    for i, facility in enumerate(facility_columns, start=1):
        value = int(row[i])

        output_cursor.execute("""
            INSERT INTO b_fz (facility, z_name, is_open)
            VALUES (?, ?, ?)
        """, (facility, z_name, value))

# =============================
# COMMIT AND CLOSE
# =============================
output_conn.commit()

model_conn.close()
output_conn.close()

print("b_fz database created successfully.")
print(f"Output DB: {OUTPUT_DB_PATH}")
print(f"Configurations processed: {len(rows)}")
print(f"Facilities processed: {len(facility_columns)}")
print(f"Rows inserted: {len(rows) * len(facility_columns)}")