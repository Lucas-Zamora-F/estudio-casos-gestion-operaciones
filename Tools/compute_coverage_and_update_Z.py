import sqlite3
import random
from pathlib import Path

import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt

from shapely.geometry import Point
from shapely.ops import unary_union
from tqdm import tqdm

# =============================
# CONFIG
# =============================

F_DB = Path("Sets/F.db")
Z_DB = Path("Sets/Z.db")

GRAPH_DIR = Path("Graphs/Coverage")
GRAPH_DIR.mkdir(parents=True, exist_ok=True)

SHAPE_PATH = Path("extern data/DPA 2024/COMUNAS/COMUNAS_v1.shp")
CENSUS_PATH = Path("extern data/CENSO/Base_manzana_entidad_CPV24.csv")

CD_RADIUS = 15000
DS_RADIUS = 5000
MDCP_RADIUS = 10000

N_RANDOM_PLOTS = 10
SEED = 42

# =============================
# HELPERS
# =============================

def create_buffer(lon: float, lat: float, radius_m: float):
    point = gpd.GeoSeries([Point(lon, lat)], crs="EPSG:4326").to_crs(epsg=32719)
    return point.iloc[0].buffer(radius_m)


def project_point_xy(lon: float, lat: float):
    point = gpd.GeoSeries([Point(lon, lat)], crs="EPSG:4326").to_crs(epsg=32719)
    geom = point.iloc[0]
    return geom.x, geom.y


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

    return gdf.to_crs(epsg=32719)


def covered_population(gdf: gpd.GeoDataFrame, geom) -> float:
    if geom is None or geom.is_empty:
        return 0.0

    temp = gdf.copy()
    temp["AREA"] = temp.geometry.area
    temp["INTER"] = temp.geometry.intersection(geom)
    temp["A_INT"] = temp["INTER"].area

    # Important fix: float column
    temp["FRAC"] = temp["A_INT"] / temp["AREA"]
    temp["FRAC"] = temp["FRAC"].fillna(0.0)

    temp["POP_COV"] = temp["POP"] * temp["FRAC"]
    return float(temp["POP_COV"].sum())


def ensure_covered_population_column(conn: sqlite3.Connection):
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(z)")
    cols = [row[1] for row in cursor.fetchall()]

    if "covered_population" not in cols:
        cursor.execute("ALTER TABLE z ADD COLUMN covered_population REAL")
        conn.commit()


# =============================
# MAIN
# =============================

def main():
    random.seed(SEED)

    # ---- load facilities ----
    conn_f = sqlite3.connect(F_DB)
    f_df = pd.read_sql("SELECT * FROM facilities", conn_f)
    conn_f.close()

    # Precompute buffers and projected XY for plotting
    buffers = {}
    projected_xy = {}

    for _, row in f_df.iterrows():
        facility_name = row["facility_name"]
        lon = float(row["longitude"])
        lat = float(row["latitude"])

        if row["type"] == "CD":
            radius = CD_RADIUS
        elif row["type"] == "DS":
            radius = DS_RADIUS
        else:
            radius = MDCP_RADIUS

        buffers[facility_name] = create_buffer(lon, lat, radius)
        projected_xy[facility_name] = project_point_xy(lon, lat)

    # ---- load geography ----
    gdf = prepare_geography()

    # ---- open Z.db ----
    conn_z = sqlite3.connect(Z_DB)
    ensure_covered_population_column(conn_z)

    z_df = pd.read_sql("SELECT * FROM z", conn_z)

    facility_cols = [
        c for c in z_df.columns
        if c not in ("z_name", "covered_population")
    ]

    populations = []

    print("Calculating p(z)...")

    for _, row in tqdm(z_df.iterrows(), total=len(z_df)):
        active = [c for c in facility_cols if int(row[c]) == 1]

        if not active:
            pop = 0.0
        else:
            geom = unary_union([buffers[a] for a in active])
            pop = covered_population(gdf, geom)

        populations.append(pop)

    # ---- update dataframe ----
    z_df["covered_population"] = populations

    # ---- write back to DB ----
    z_df.to_sql("z", conn_z, if_exists="replace", index=False)
    conn_z.close()

    print("Z.db updated with covered_population")

    # =============================
    # HISTOGRAM
    # =============================
    plt.figure(figsize=(10, 6))
    plt.hist(populations, bins=50)
    plt.title("Distribution of Covered Population")
    plt.xlabel("Covered Population")
    plt.ylabel("Frequency")
    plt.tight_layout()
    plt.savefig(GRAPH_DIR / "coverage_histogram.png", dpi=300)
    plt.close()

    print("Histogram saved")

    # =============================
    # RANDOM MAPS
    # =============================
    sample_size = min(N_RANDOM_PLOTS, len(z_df))
    sample = z_df.sample(sample_size, random_state=SEED)

    for _, row in sample.iterrows():
        active = [c for c in facility_cols if int(row[c]) == 1]

        if not active:
            continue

        fig, ax = plt.subplots(figsize=(10, 10))

        # Base geography
        gdf.plot(
            column="POP",
            ax=ax,
            legend=True,
            cmap="viridis",
            linewidth=0.4,
            edgecolor="black"
        )

        # Coverage buffers
        for facility in active:
            gpd.GeoSeries([buffers[facility]], crs="EPSG:32719").plot(
                ax=ax,
                alpha=0.30
            )

        # Facility points and labels
        for facility in active:
            x, y = projected_xy[facility]
            ax.scatter(x, y, color="black", s=18, zorder=5)
            ax.text(x, y, facility, fontsize=8, zorder=6)

        ax.set_title(
            f"{row['z_name']} | Active: {len(active)} | "
            f"Pop: {int(row['covered_population']):,}"
        )

        ax.set_axis_off()
        plt.tight_layout()
        plt.savefig(GRAPH_DIR / f"{row['z_name']}.png", dpi=300)
        plt.close()

    print("Random maps saved")


if __name__ == "__main__":
    main()