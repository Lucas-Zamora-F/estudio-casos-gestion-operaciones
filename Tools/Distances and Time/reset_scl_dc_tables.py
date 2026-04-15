import sqlite3
from pathlib import Path

TRANSPORT_DB = Path("Sets/transport_matrices.db")


def main():
    print("\n" + "=" * 60)
    print("RESET S_cl_DC TABLES")
    print("=" * 60)

    conn = sqlite3.connect(TRANSPORT_DB)
    cur = conn.cursor()

    # Borrar tablas viejas
    cur.execute('DROP TABLE IF EXISTS "S_cl_DC_distance"')
    cur.execute('DROP TABLE IF EXISTS "S_cl_DC_time"')

    # Crear tablas nuevas con PK compuesta
    cur.execute("""
        CREATE TABLE "S_cl_DC_distance" (
            origin_name TEXT NOT NULL,
            destination_name TEXT NOT NULL,
            distance_km REAL,
            PRIMARY KEY (origin_name, destination_name)
        )
    """)

    cur.execute("""
        CREATE TABLE "S_cl_DC_time" (
            origin_name TEXT NOT NULL,
            destination_name TEXT NOT NULL,
            time_min REAL,
            PRIMARY KEY (origin_name, destination_name)
        )
    """)

    conn.commit()
    conn.close()

    print("[OK] Tables recreated successfully.")


if __name__ == "__main__":
    main()