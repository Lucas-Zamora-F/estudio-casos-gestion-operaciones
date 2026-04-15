import sqlite3
from pathlib import Path

# ============================================================
# CONFIG
# ============================================================
DB_PATH = Path("Sets/model.db")

NEW_LAT = -29.969886
NEW_LON = -71.337173

TARGET_ORIGIN = "Chile-Coq"


# ============================================================
# MAIN
# ============================================================
def main():
    print("\n" + "=" * 60)
    print("FIX CHILE-COQ COORDINATES")
    print("=" * 60)

    if not DB_PATH.exists():
        raise FileNotFoundError(f"Database not found: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Verificar existencia
    cursor.execute("""
        SELECT origin, latitude, longitude
        FROM S_cl
        WHERE origin = ?
    """, (TARGET_ORIGIN,))

    row = cursor.fetchone()

    if row is None:
        print(f"[ERROR] {TARGET_ORIGIN} not found in S_cl")
        conn.close()
        return

    print("\nBEFORE:")
    print(f"Origin     : {row[0]}")
    print(f"Latitude   : {row[1]}")
    print(f"Longitude  : {row[2]}")

    # Update
    cursor.execute("""
        UPDATE S_cl
        SET latitude = ?, longitude = ?
        WHERE origin = ?
    """, (NEW_LAT, NEW_LON, TARGET_ORIGIN))

    conn.commit()

    # Verificar cambio
    cursor.execute("""
        SELECT origin, latitude, longitude
        FROM S_cl
        WHERE origin = ?
    """, (TARGET_ORIGIN,))

    row_after = cursor.fetchone()

    print("\nAFTER:")
    print(f"Origin     : {row_after[0]}")
    print(f"Latitude   : {row_after[1]}")
    print(f"Longitude  : {row_after[2]}")

    conn.close()

    print("\n[OK] Coordinates updated successfully.")


if __name__ == "__main__":
    main()
    