import sqlite3
from pathlib import Path


# =========================
# File paths
# =========================
DB_PATH = Path("Sets/model.db")


# =========================
# Data
# =========================
E_DATA = [
    ("Arturo Merino Benitez International Airport", "Airport", -33.392, -70.785),
    ("San Antonio", "Port", -33.593, -71.621),
    ("Valparaiso", "Port", -33.047, -71.612),
    ("Chacalluta", "Land customs", -18.348, -70.338),
]

WM_DATA = [
    ("La Vega Central", -33.419, -70.648),
    ("Lo Valledor", -33.504, -70.699),
]

S_CL_DATA = [
    ("Chile-Scl", -33.448, -70.669),
    ("Chile-Vap", -33.047, -71.612),
    ("Chile-Maul", -35.426, -71.655),
    ("Chile-Coq", -30.248, -71.336),
]

S_IMP_DATA = [
    ("US", 37.090, -95.712),
    ("Mexico", 23.634, -102.553),
    ("Peru", -9.190, -75.015),
    ("Ecuador", -1.831, -78.183),
    ("Bolivia", -16.290, -63.588),
    ("Argentina", -38.417, -63.616),
    ("Spain", 40.463, -3.749),
]

K_DATA = [
    ("Roma Tomatoes",),
    ("Cauliflower",),
    ("Broccoli",),
    ("Asparagus",),
    ("Green Bell Pepper",),
]


# =========================
# Main
# =========================
def main() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # -------------------------
    # Drop tables if they exist
    # -------------------------
    cursor.execute("DROP TABLE IF EXISTS E")
    cursor.execute("DROP TABLE IF EXISTS WM")
    cursor.execute("DROP TABLE IF EXISTS S_cl")
    cursor.execute("DROP TABLE IF EXISTS S_imp")
    cursor.execute("DROP TABLE IF EXISTS K")

    # -------------------------
    # Create table E
    # Note:
    # The column name "international entry point"
    # contains spaces, so it must be quoted.
    # -------------------------
    cursor.execute("""
        CREATE TABLE E (
            "international entry point" TEXT PRIMARY KEY,
            type TEXT NOT NULL,
            latitude REAL NOT NULL,
            longitude REAL NOT NULL
        )
    """)

    cursor.executemany("""
        INSERT INTO E ("international entry point", type, latitude, longitude)
        VALUES (?, ?, ?, ?)
    """, E_DATA)

    # -------------------------
    # Create table WM
    # -------------------------
    cursor.execute("""
        CREATE TABLE WM (
            wholesale_market TEXT PRIMARY KEY,
            latitude REAL NOT NULL,
            longitude REAL NOT NULL
        )
    """)

    cursor.executemany("""
        INSERT INTO WM (wholesale_market, latitude, longitude)
        VALUES (?, ?, ?)
    """, WM_DATA)

    # -------------------------
    # Create table S_cl
    # -------------------------
    cursor.execute("""
        CREATE TABLE S_cl (
            origin TEXT PRIMARY KEY,
            latitude REAL NOT NULL,
            longitude REAL NOT NULL
        )
    """)

    cursor.executemany("""
        INSERT INTO S_cl (origin, latitude, longitude)
        VALUES (?, ?, ?)
    """, S_CL_DATA)

    # -------------------------
    # Create table S_imp
    # -------------------------
    cursor.execute("""
        CREATE TABLE S_imp (
            origin TEXT PRIMARY KEY,
            latitude REAL NOT NULL,
            longitude REAL NOT NULL
        )
    """)

    cursor.executemany("""
        INSERT INTO S_imp (origin, latitude, longitude)
        VALUES (?, ?, ?)
    """, S_IMP_DATA)

    # -------------------------
    # Create table K
    # -------------------------
    cursor.execute("""
        CREATE TABLE K (
            product TEXT PRIMARY KEY
        )
    """)

    cursor.executemany("""
        INSERT INTO K (product)
        VALUES (?)
    """, K_DATA)

    conn.commit()
    conn.close()

    print(f"Tables created successfully in: {DB_PATH}")
    print("Created tables: E, WM, S_cl, S_imp, K")


if __name__ == "__main__":
    main()