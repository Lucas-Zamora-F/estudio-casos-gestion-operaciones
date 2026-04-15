import sqlite3
import pandas as pd
from pathlib import Path

MODEL_DB = Path("Sets/model.db")
PARAM_DB = Path("Sets/parameters.db")

# =============================
# LOAD TABLES
# =============================
conn_model = sqlite3.connect(MODEL_DB)
df_E = pd.read_sql_query('SELECT * FROM "E"', conn_model)
conn_model.close()

conn_param = sqlite3.connect(PARAM_DB)
df_Cpurimp = pd.read_sql_query('SELECT * FROM "C_pur_imp"', conn_param)
conn_param.close()

# =============================
# CLEAN TEXT
# =============================
for df in [df_E, df_Cpurimp]:
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].astype(str).str.strip()

# =============================
# 1. VER TIPOS REALES DE E
# =============================
print("\n==============================")
print("TIPOS DE ENTRY POINT (E.type)")
print("==============================")
print(df_E["type"].unique())

print("\nDetalle:")
print(df_E[["international entry point", "type"]])

# =============================
# 2. VER TABLA DE COSTOS
# =============================
print("\n==============================")
print("C_Pur_Imp (primeras filas)")
print("==============================")
print(df_Cpurimp.head(20))

print("\nColumnas:")
print(df_Cpurimp.columns.tolist())

# =============================
# 3. DETECTAR COSTOS NULOS / CERO
# =============================
print("\n==============================")
print("VALORES SOSPECHOSOS EN C_Pur_Imp")
print("==============================")

for col in [
    "purchase_cost_usd_per_kg_sea",
    "purchase_cost_usd_per_kg_air",
    "purchase_cost_usd_per_kg_land",
]:
    nulls = df_Cpurimp[df_Cpurimp[col].isna()]
    zeros = df_Cpurimp[df_Cpurimp[col] == 0]

    print(f"\n--- {col} ---")
    print(f"NaN rows: {len(nulls)}")
    if len(nulls) > 0:
        print(nulls[["product", "origin", col]])

    print(f"Zero rows: {len(zeros)}")
    if len(zeros) > 0:
        print(zeros[["product", "origin", col]])

# =============================
# 4. TEST DE MATCH REAL
# =============================
print("\n==============================")
print("TEST DE MATCH ENTRE E.type Y COSTOS")
print("==============================")

# Mapeo que usa tu modelo
map_type = {
    "Port": "purchase_cost_usd_per_kg_sea",
    "Airport": "purchase_cost_usd_per_kg_air",
    "Land customs": "purchase_cost_usd_per_kg_land",
}

products = df_Cpurimp["product"].unique()
origins = df_Cpurimp["origin"].unique()

for _, row_e in df_E.iterrows():
    e = row_e["international entry point"]
    e_type = row_e["type"]

    if e_type not in map_type:
        print(f"\n[ERROR] Tipo NO reconocido: '{e_type}' en entry point '{e}'")
        continue

    col = map_type[e_type]

    print(f"\nEntry: {e} | Type: {e_type} -> uses column: {col}")

    # revisar algunos ejemplos
    for k in products[:3]:  # solo algunos para no spamear
        for s in origins[:3]:
            row = df_Cpurimp[
                (df_Cpurimp["product"] == k) &
                (df_Cpurimp["origin"] == s)
            ]

            if len(row) == 0:
                print(f"  [MISSING ROW] product={k}, origin={s}")
                continue

            val = row.iloc[0][col]

            print(f"  product={k}, origin={s} -> cost={val}")