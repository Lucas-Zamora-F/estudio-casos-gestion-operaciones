import sqlite3
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import random

from pathlib import Path
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

def create_buffer(lon, lat, r):
    point = gpd.GeoSeries([Point(lon, lat)], crs="EPSG:4326").to_crs(epsg=32719)
    return point.iloc[0].buffer(r)

def prepare_geography():
    gdf = gpd.read_file(SHAPE_PATH)

    gdf["CUT_REG"] = gdf["CUT_REG"].astype(str).str.zfill(2)
    gdf = gdf[gdf["CUT_REG"] == "13"].copy()

    df = pd.read_csv(CENSUS_PATH, sep=";", low_memory=False)
    df["COD_REGION"] = pd.to_numeric(df["COD_REGION"], errors="coerce")
    df = df[df["COD_REGION"] == 13]

    df["CUT"] = df["CUT"].astype(str).str.zfill(5)

    df_com = df.groupby("CUT", as_index=False)["n_per"].sum()
    df_com = df_com.rename(columns={"n_per": "POP"})

    gdf = gdf.merge(df_com, left_on="CUT_COM", right_on="CUT", how="left")
    gdf["POP"] = gdf["POP"].fillna(0)

    return gdf.to_crs(epsg=32719)

def covered_population(gdf, geom):
    if geom is None or geom.is_empty:
        return 0.0

    temp = gdf.copy()
    temp["AREA"] = temp.geometry.area
    temp["INTER"] = temp.geometry.intersection(geom)
    temp["A_INT"] = temp["INTER"].area

    temp["FRAC"] = 0
    mask = temp["AREA"] > 0
    temp.loc[mask, "FRAC"] = temp["A_INT"] / temp["AREA"]

    temp["POP_COV"] = temp["POP"] * temp["FRAC"]

    return float(temp["POP_COV"].sum())

# =============================
# MAIN
# =============================

def main():

    random.seed(SEED)

    # ---- load facilities ----
    conn = sqlite3.connect(F_DB)
    f_df = pd.read_sql("SELECT * FROM facilities", conn)
    conn.close()

    buffers = {}

    for _, row in f_df.iterrows():
        if row["type"] == "CD":
            r = CD_RADIUS
        elif row["type"] == "DS":
            r = DS_RADIUS
        else:
            r = MDCP_RADIUS

        buffers[row["facility_name"]] = create_buffer(
            row["longitude"], row["latitude"], r
        )

    # ---- load geography ----
    gdf = prepare_geography()

    # ---- open Z.db ----
    conn = sqlite3.connect(Z_DB)
    z_df = pd.read_sql("SELECT * FROM z", conn)

    facility_cols = [c for c in z_df.columns if c != "z_name"]

    populations = []

    print("Calculating p(z)...")

    for _, row in tqdm(z_df.iterrows(), total=len(z_df)):

        active = [c for c in facility_cols if row[c] == 1]

        if not active:
            pop = 0.0
        else:
            geom = unary_union([buffers[a] for a in active])
            pop = covered_population(gdf, geom)

        populations.append(pop)

    # ---- update DB ----
    z_df["covered_population"] = populations

    z_df.to_sql("z", conn, if_exists="replace", index=False)
    conn.close()

    print("Z.db updated with covered_population")

    # =============================
    # HISTOGRAM
    # =============================

    plt.figure(figsize=(10,6))
    plt.hist(populations, bins=50)
    plt.title("Distribution of Covered Population")
    plt.xlabel("Covered Population")
    plt.ylabel("Frequency")

    plt.savefig(GRAPH_DIR / "coverage_histogram.png")
    plt.close()

    print("Histogram saved")

    # =============================
    # RANDOM MAPS
    # =============================

    sample = z_df.sample(N_RANDOM_PLOTS)

    for _, row in sample.iterrows():

        active = [c for c in facility_cols if row[c] == 1]

        if not active:
            continue

        geom = unary_union([buffers[a] for a in active])

        fig, ax = plt.subplots(figsize=(8,8))

        gdf.plot(column="POP", ax=ax, legend=True)

        for a in active:
            gpd.GeoSeries([buffers[a]]).plot(ax=ax, alpha=0.3)

        for a in active:
            r = f_df[f_df["facility_name"] == a].iloc[0]
            ax.scatter(r["longitude"], r["latitude"], color="black")
            ax.text(r["longitude"], r["latitude"], a, fontsize=8)

        ax.set_title(
            f"{row['z_name']} | Active: {len(active)} | Pop: {int(row['covered_population'])}"
        )

        plt.savefig(GRAPH_DIR / f"{row['z_name']}.png")
        plt.close()

    print("Random maps saved")


if __name__ == "__main__":
    main()