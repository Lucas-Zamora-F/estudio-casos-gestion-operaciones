from __future__ import annotations

import random
from pathlib import Path

import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt

from shapely.geometry import Point
from shapely.ops import unary_union
from tqdm import tqdm


# ==========================================
# CONFIGURATION
# ==========================================

BASE_DIR = Path("data") / "SOLVER DATA"
GRAPH_DIR = Path("graphs")

SHAPE_PATH = Path("data") / "DPA 2024" / "COMUNAS" / "COMUNAS_v1.shp"
CENSUS_CSV_PATH = Path("data") / "CENSO" / "Base_manzana_entidad_CPV24.csv"

CD_PATH = BASE_DIR / "CD.csv"
DS_PATH = BASE_DIR / "DS.csv"
MDCP_PATH = BASE_DIR / "MDCP.csv"
Z_PATH = BASE_DIR / "Z.csv"
OUTPUT_PATH = BASE_DIR / "p(z).csv"

CD_RADIUS_M = 15000
DS_RADIUS_M = 5000
MDCP_RADIUS_M = 10000

N_PLOTS = 20
SEED = 42
CHUNK_SIZE = 5000

ZOOM_PADDING_M = 8000


# ==========================================
# AUX FUNCTIONS
# ==========================================

def create_buffer(lon: float, lat: float, radius_m: float):
    point = gpd.GeoSeries([Point(lon, lat)], crs="EPSG:4326").to_crs(epsg=32719)
    if radius_m <= 0:
        return point.iloc[0]
    return point.iloc[0].buffer(radius_m)


def load_facilities(path: Path, name_col: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    expected_cols = [name_col, "latitude", "longitude"]
    missing = [c for c in expected_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in {path}: {missing}")
    return df[expected_cols].copy()


def covered_population(gdf_base: gpd.GeoDataFrame, geom_union) -> float:
    if geom_union is None or geom_union.is_empty:
        return 0.0

    temp = gdf_base.copy()
    temp["AREA_TOTAL"] = temp.geometry.area
    temp["INTER"] = temp.geometry.intersection(geom_union)
    temp["AREA_INTER"] = temp["INTER"].area

    temp["FRAC"] = 0.0
    mask = temp["AREA_TOTAL"] > 0
    temp.loc[mask, "FRAC"] = temp.loc[mask, "AREA_INTER"] / temp.loc[mask, "AREA_TOTAL"]

    temp["POP_COVERED"] = temp["POBLACION_TOTAL"] * temp["FRAC"]
    return float(temp["POP_COVERED"].sum())


def prepare_geography() -> gpd.GeoDataFrame:
    gdf = gpd.read_file(SHAPE_PATH)
    gdf["CUT_REG"] = gdf["CUT_REG"].astype(str).str.zfill(2)
    gdf["CUT_COM"] = gdf["CUT_COM"].astype(str).str.zfill(5)
    gdf = gdf[gdf["CUT_REG"] == "13"].copy()

    df = pd.read_csv(CENSUS_CSV_PATH, sep=";", low_memory=False)
    df["COD_REGION"] = pd.to_numeric(df["COD_REGION"], errors="coerce")
    df_rm = df[df["COD_REGION"] == 13].copy()
    df_rm["CUT"] = df_rm["CUT"].astype(str).str.zfill(5)

    df_comunas = (
        df_rm.groupby(["CUT", "COMUNA"], as_index=False)["n_per"]
        .sum()
        .rename(columns={"n_per": "POBLACION_TOTAL"})
    )

    gdf = gdf.merge(
        df_comunas[["CUT", "POBLACION_TOTAL"]],
        left_on="CUT_COM",
        right_on="CUT",
        how="left"
    )

    gdf["POBLACION_TOTAL"] = gdf["POBLACION_TOTAL"].fillna(0)
    gdf = gdf.to_crs(epsg=32719)

    return gdf


def build_buffers(df_cd, df_ds, df_mdcp):
    buffers = {}
    coords = {}

    for _, row in df_cd.iterrows():
        name = str(row["cd_name"])
        lat = float(row["latitude"])
        lon = float(row["longitude"])
        buffers[name] = create_buffer(lon, lat, CD_RADIUS_M)
        coords[name] = (lon, lat)

    for _, row in df_ds.iterrows():
        name = str(row["ds_name"])
        lat = float(row["latitude"])
        lon = float(row["longitude"])
        buffers[name] = create_buffer(lon, lat, DS_RADIUS_M)
        coords[name] = (lon, lat)

    for _, row in df_mdcp.iterrows():
        name = str(row["mdcp_name"])
        lat = float(row["latitude"])
        lon = float(row["longitude"])
        buffers[name] = create_buffer(lon, lat, MDCP_RADIUS_M)
        coords[name] = (lon, lat)

    return buffers, coords


def count_z_rows(path: Path) -> int:
    with open(path, "r", encoding="utf-8-sig") as f:
        total = sum(1 for _ in f) - 1
    return max(total, 0)


def build_color_map(cd, ds, mdcp):
    palette = [
        "tab:blue", "tab:orange", "tab:green", "tab:red", "tab:purple",
        "tab:brown", "tab:pink", "tab:gray", "tab:olive", "tab:cyan"
    ]

    names = cd + ds + mdcp
    return {name: palette[i] for i, name in enumerate(names)}


# ==========================================
# MAIN
# ==========================================

def main():
    random.seed(SEED)
    GRAPH_DIR.mkdir(parents=True, exist_ok=True)

    print("==========================================")
    print("p(z) CALCULATION")
    print("==========================================")

    df_cd = load_facilities(CD_PATH, "cd_name")
    df_ds = load_facilities(DS_PATH, "ds_name")
    df_mdcp = load_facilities(MDCP_PATH, "mdcp_name")

    cd_names = df_cd["cd_name"].tolist()
    ds_names = df_ds["ds_name"].tolist()
    mdcp_names = df_mdcp["mdcp_name"].tolist()

    expected_cols = cd_names + ds_names + mdcp_names

    gdf_base = prepare_geography()
    buffers, coords = build_buffers(df_cd, df_ds, df_mdcp)

    total_rows = count_z_rows(Z_PATH)

    z_header = pd.read_csv(Z_PATH, nrows=0).columns.tolist()
    if z_header[0] != "z_name":
        raise ValueError("First column must be 'z_name'.")

    facility_cols = z_header[1:]

    if facility_cols != expected_cols:
        raise ValueError("Z.csv columns do not match expected order.")

    if OUTPUT_PATH.exists():
        OUTPUT_PATH.unlink()

    first_write = True
    row_global = 0

    for chunk in pd.read_csv(Z_PATH, chunksize=CHUNK_SIZE):
        results = []

        for _, row in chunk.iterrows():
            row_global += 1
            z_name = row["z_name"]

            active = [col for col in facility_cols if int(row[col]) == 1]

            if not active:
                pop = 0.0
            else:
                union_geom = unary_union([buffers[n] for n in active])
                pop = covered_population(gdf_base, union_geom)

            results.append({
                "z": z_name,
                "population": round(pop, 4)
            })

        pd.DataFrame(results).to_csv(
            OUTPUT_PATH,
            mode="w" if first_write else "a",
            header=first_write,
            index=False,
            encoding="utf-8-sig"
        )
        first_write = False

    print(f"[OK] Output saved at: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()