# ============================================
# Script: create_C_open_table.py
# ============================================

import sqlite3
from pathlib import Path

# =============================
# CONFIG
# =============================
DB_PATH = Path("Sets/parameters.db")

# =============================
# HARDCODED DATA
# C_open[f] = terreno + costo estratégico
# =============================
data = [
    # -------- CDs --------
    ("DC_Maipu", 5726500.00),
    ("DC_Conchali", 8149250.00),
    ("DC_Macul", 8810000.00),
    ("DC_Quilicura", 6465540.00),
    ("DC_San_Bernardo", 8369500.00),

    # -------- DS --------
    ("DS_San_Miguel", 991125.00),
    ("DS_Providencia", 969100.00),
    ("DS_Puente_Alto", 884499.33 + 120389.44),
    ("DS_Independencia", 2334209.50),
    ("DS_Lo_Espejo", 1409600.00),
    ("DS_Conchali", 2246550.00),
    ("DS_Estacion_Central", 9470750.00),
    ("DS_Cerro_Navia", 660750.00),
    ("DS_Lo_Barnechea", 2211248.33 + 12325.00),
    ("DS_La_Florida", 202630.00 + 21960.00),

    # -------- MDCP --------
    ("MDCP_Pudahuel", 1629.85),
    ("MDCP_Quilicura", 374425.00),
    ("MDCP_Providencia", 969100.00 + 150.00),
    ("MDCP_San_Bernardo", 2766340.00),
]

# =============================
# CONNECT
# =============================
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# =============================
# CREATE TABLE
# =============================
cursor.execute("DROP TABLE IF EXISTS C_open")

cursor.execute("""
    CREATE TABLE C_open (
        facility TEXT PRIMARY KEY,
        cost_usd REAL
    )
""")

print("[OK] Table C_open created.")

# =============================
# INSERT DATA
# =============================
cursor.executemany("""
    INSERT INTO C_open (facility, cost_usd)
    VALUES (?, ?)
""", data)

print(f"[OK] Inserted {len(data)} rows into C_open.")

# =============================
# COMMIT & CLOSE
# =============================
conn.commit()
conn.close()

print("[DONE] parameters.db updated successfully.")