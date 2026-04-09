import sqlite3
from pathlib import Path

import pandas as pd
import geopandas as gpd

from shapely.geometry import Point
from shapely.ops import unary_union
from tqdm import tqdm

# =============================
# FILE NAME
# =============================
# update_Z_coverage_columns.py

# =============================
# CONFIG
# =============================

DB_PATH = Path("Sets/model.db")
SHAPE_PATH = Path("extern data/DPA 2024/COMUNAS/COMUNAS_v1.shp")
CENSUS_PATH = Path("extern data/CENSO/Base_manzana_entidad_CPV24.csv")

DC_RADIUS = 15000
DS_RADIUS = 5000
MDCP_RADIUS = 10000

PERSONS_PER_HOUSEHOLD = 2.8

# =============================
# HELPERS
# =============================

def create_buffer(lon: float, lat: float, radius_m: float):
    point = gpd.GeoSeries([Point(lon, lat)], crs="EPSG:4326").to_crs(epsg=32719)
    return point.iloc[0].buffer(radius_m)


def prepare_geography():
    gdf = gpd.read_file(SHAPE_PATH)

    gdf["CUT_REG"] = gdf["CUT_REG"].astype(str).str.zfill(2)
    gdf["CUT_COM"] = gdf["CUT_COM"].astype(str).str.zfill(5)
    gdf = gdf[gdf["CUT_REG"] == "13"].copy()

    df = pd.read_csv(CENSUS_PATH, sep=";", low_memory=False)
    df["COD_REGION"] = pd.to_numeric(df["COD_REGION"], errors="coerce")
    df = df[df["COD_REGION"] == 13].copy()
    df["CUT"] = df["CUT"].astype(str).str.zfill(5)

    df_com = (
        df.groupby("CUT", as_index=False)["n_per"]
        .sum()
        .rename(columns={"n_per": "POP"})
    )

    gdf = gdf.merge(df_com, left_on="CUT_COM", right_on="CUT", how="left")
    gdf["POP"] = gdf["POP"].fillna(0.0)
    gdf["HOUSEHOLDS"] = gdf["POP"] / PERSONS_PER_HOUSEHOLD

    return gdf.to_crs(epsg=32719)


def covered_population(gdf: gpd.GeoDataFrame, geom) -> float:
    if geom is None or geom.is_empty:
        return 0.0

    temp = gdf.copy()
    temp["AREA"] = temp.geometry.area
    temp["INTER"] = temp.geometry.intersection(geom)
    temp["A_INT"] = temp["INTER"].area

    temp["FRAC"] = temp["A_INT"] / temp["AREA"]
    temp["FRAC"] = temp["FRAC"].fillna(0.0)

    temp["POP_COV"] = temp["POP"] * temp["FRAC"]
    return float(temp["POP_COV"].sum())


def covered_households(gdf: gpd.GeoDataFrame, geom) -> float:
    if geom is None or geom.is_empty:
        return 0.0

    temp = gdf.copy()
    temp["AREA"] = temp.geometry.area
    temp["INTER"] = temp.geometry.intersection(geom)
    temp["A_INT"] = temp["INTER"].area

    temp["FRAC"] = temp["A_INT"] / temp["AREA"]
    temp["FRAC"] = temp["FRAC"].fillna(0.0)

    temp["HOUSEHOLDS_COV"] = temp["HOUSEHOLDS"] * temp["FRAC"]
    return float(temp["HOUSEHOLDS_COV"].sum())


def ensure_coverage_columns(conn: sqlite3.Connection):
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(Z)")
    cols = [row[1] for row in cursor.fetchall()]

    if "covered_population" not in cols:
        cursor.execute("ALTER TABLE Z ADD COLUMN covered_population REAL")

    if "covered_households" not in cols:
        cursor.execute("ALTER TABLE Z ADD COLUMN covered_households REAL")

    conn.commit()


def get_radius_by_type(facility_type: str) -> float:
    if facility_type == "DC":
        return DC_RADIUS
    if facility_type == "DS":
        return DS_RADIUS
    if facility_type == "MDCP":
        return MDCP_RADIUS
    raise ValueError(f"Unknown facility type: {facility_type}")


# =============================
# MAIN
# =============================

def main():
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Database not found: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)

    # ---- load facilities from F ----
    f_df = pd.read_sql("""
        SELECT facility_name, type, latitude, longitude
        FROM F
        ORDER BY facility_name
    """, conn)

    if f_df.empty:
        conn.close()
        raise ValueError("Table F is empty.")

    buffers = {}

    for _, row in f_df.iterrows():
        facility_name = row["facility_name"]
        lon = float(row["longitude"])
        lat = float(row["latitude"])
        facility_type = row["type"]

        radius = get_radius_by_type(facility_type)
        buffers[facility_name] = create_buffer(lon, lat, radius)

    # ---- load geography ----
    gdf = prepare_geography()

    # ---- ensure columns exist in Z ----
    ensure_coverage_columns(conn)

    # ---- load Z ----
    z_df = pd.read_sql("SELECT * FROM Z", conn)

    if z_df.empty:
        conn.close()
        raise ValueError("Table Z is empty.")

    facility_cols = [
        c for c in z_df.columns
        if c not in ("z_name", "covered_population", "covered_households")
    ]

    missing_in_f = [c for c in facility_cols if c not in set(f_df["facility_name"])]
    if missing_in_f:
        conn.close()
        raise ValueError(
            "These facility columns exist in Z but not in F: "
            + ", ".join(missing_in_f)
        )

    update_rows = []

    print("Calculating covered_population and covered_households for each z...")

    for _, row in tqdm(z_df.iterrows(), total=len(z_df)):
        active = [c for c in facility_cols if int(row[c]) == 1]

        if not active:
            pop = 0.0
            hh = 0.0
        else:
            geom = unary_union([buffers[a] for a in active])
            pop = covered_population(gdf, geom)
            hh = covered_households(gdf, geom)

        update_rows.append((pop, hh, row["z_name"]))

    cursor = conn.cursor()
    cursor.executemany("""
        UPDATE Z
        SET covered_population = ?, covered_households = ?
        WHERE z_name = ?
    """, update_rows)

    conn.commit()
    conn.close()

    print("Table Z updated successfully inside Sets/model.db.")


if __name__ == "__main__":
    main()