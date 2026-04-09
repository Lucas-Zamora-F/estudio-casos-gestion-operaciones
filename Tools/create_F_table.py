import sqlite3
from pathlib import Path

# =============================
# CONFIG
# =============================

DB_PATH = Path("Sets/model.db")

# =============================
# DATA
# =============================

data = [
    ("DC_Maipu","DC",-33.530964,-70.764846),
    ("DC_Conchali","DC",-33.373221,-70.695362),
    ("DC_Macul","DC",-33.484915,-70.612430),
    ("DC_Quilicura","DC",-33.344485,-70.709559),
    ("DC_San_Bernardo","DC",-33.678413,-70.728591),
    ("DS_Providencia","DS",-33.432897,-70.630474),
    ("DS_San_Miguel","DS",-33.477581,-70.641970),
    ("DS_Puente_Alto","DS",-33.606396,-70.563127),
    ("DS_La_Florida","DS",-33.561758,-70.578209),
    ("DS_Lo_Barnechea","DS",-33.359486,-70.515298),
    ("DS_Independencia","DS",-33.423766,-70.662034),
    ("DS_Lo_Espejo","DS",-33.534500,-70.692455),
    ("DS_Conchali","DS",-33.378644,-70.693355),
    ("DS_Cerro_Navia","DS",-33.422698,-70.740202),
    ("DS_Estacion_Central","DS",-33.452440,-70.683859),
    ("MDCP_Pudahuel","MDCP",-33.459410,-70.864273),
    ("MDCP_Quilicura","MDCP",-33.374762,-70.714256),
    ("MDCP_Providencia","MDCP",-33.433112,-70.630817),
    ("MDCP_San_Bernardo","MDCP",-33.585767,-70.699079),
]

# =============================
# CREATE TABLE + INSERT
# =============================

def main():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Crear tabla
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS F (
        facility_name TEXT PRIMARY KEY,
        type TEXT CHECK(type IN ('DC','DS','MDCP')),
        latitude REAL,
        longitude REAL
    )
    """)

    # Limpiar tabla (opcional pero recomendable si corres varias veces)
    cursor.execute("DELETE FROM F")

    # Insertar datos
    cursor.executemany("""
    INSERT INTO F (facility_name, type, latitude, longitude)
    VALUES (?, ?, ?, ?)
    """, data)

    conn.commit()
    conn.close()

    print("Table F created and populated successfully.")

# =============================
# RUN
# =============================

if __name__ == "__main__":
    main()