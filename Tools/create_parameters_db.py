import sqlite3
from pathlib import Path

# =============================
# CONFIG
# =============================
DB_PATH = Path("Sets/parameters.db")
MODEL_DB_PATH = Path("Sets/model.db")

DB_PATH.parent.mkdir(parents=True, exist_ok=True)

# =============================
# CONNECT
# =============================
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# =============================
# CREATE TABLES
# =============================

# Purchase costs
cursor.execute("""
CREATE TABLE IF NOT EXISTS C_pur_imp (
    product TEXT,
    supplier TEXT,
    entry_point TEXT,
    cost_usd_per_kg REAL
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS C_pur_cl (
    product TEXT,
    supplier TEXT,
    cost_usd_per_kg REAL
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS C_pur_wm (
    product TEXT,
    market TEXT,
    cost_usd_per_kg REAL
)
""")

# Facility costs
cursor.execute("""
CREATE TABLE IF NOT EXISTS C_open (
    facility TEXT,
    cost_usd REAL
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS C_op_fix (
    facility TEXT,
    cost_usd_per_period REAL
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS C_op_var (
    facility TEXT,
    cost_usd_per_kg REAL
)
""")

# Transport costs
cursor.execute("""
CREATE TABLE IF NOT EXISTS C_tr (
    mode TEXT,
    cost_usd_per_km REAL
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS C_lm (
    mode TEXT,
    cost_usd_per_km_kg REAL
)
""")

# Amortization
cursor.execute("""
CREATE TABLE IF NOT EXISTS Y_amor (
    asset_type TEXT,
    years INTEGER
)
""")

# Configuration matrix
cursor.execute("""
CREATE TABLE IF NOT EXISTS b_fz (
    facility TEXT,
    z INTEGER,
    value INTEGER
)
""")

# =============================
# NEW TABLES
# =============================

# Product prices (selling price)
cursor.execute("""
CREATE TABLE IF NOT EXISTS P_k (
    product TEXT PRIMARY KEY,
    price_usd_per_kg REAL
)
""")

# Delivery fee
cursor.execute("""
CREATE TABLE IF NOT EXISTS F_del (
    id INTEGER PRIMARY KEY,
    fee_usd REAL
)
""")

# =============================
# OPTIONAL: LOAD FROM model.db
# =============================

if MODEL_DB_PATH.exists():
    model_conn = sqlite3.connect(MODEL_DB_PATH)
    model_cursor = model_conn.cursor()

    # Facilities
    try:
        model_cursor.execute("SELECT facility_name FROM F")
        facilities = [row[0] for row in model_cursor.fetchall()]
        for f in facilities:
            cursor.execute("INSERT INTO C_open VALUES (?, ?)", (f, 0.0))
            cursor.execute("INSERT INTO C_op_fix VALUES (?, ?)", (f, 0.0))
            cursor.execute("INSERT INTO C_op_var VALUES (?, ?)", (f, 0.0))
    except:
        pass

    # Z configurations
    try:
        model_cursor.execute("SELECT z FROM Z")
        zs = [row[0] for row in model_cursor.fetchall()]
        for f in facilities:
            for z in zs:
                cursor.execute("INSERT INTO b_fz VALUES (?, ?, ?)", (f, z, 0))
    except:
        pass

    model_conn.close()

# =============================
# INSERT DEFAULT VALUES
# =============================

# Transport defaults
cursor.execute("INSERT INTO C_tr VALUES ('truck', 1.5)")
cursor.execute("INSERT INTO C_lm VALUES ('last_mile', 0.02)")

# Amortization defaults
cursor.execute("INSERT INTO Y_amor VALUES ('vehicle', 5)")
cursor.execute("INSERT INTO Y_amor VALUES ('facility', 10)")

# Product prices (ejemplo inicial)
products = ["tomato", "cauliflower", "broccoli", "asparagus", "pepper"]
for p in products:
    cursor.execute("INSERT OR IGNORE INTO P_k VALUES (?, ?)", (p, 0.0))

# Delivery fee (según enunciado ≈ 2.5 USD)
cursor.execute("INSERT INTO F_del VALUES (1, 2.5)")

# =============================
# COMMIT & CLOSE
# =============================
conn.commit()
conn.close()

print("parameters.db creado correctamente con todas las tablas.")