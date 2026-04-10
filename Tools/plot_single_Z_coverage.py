import sqlite3
from pathlib import Path

import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt

from shapely.geometry import Point
from shapely.ops import unary_union


# =============================
# FILE NAME
# =============================
# plot_single_Z_coverage.py

# =============================
# USER INPUT (HARDCODED)
# =============================
Z_INDEX = 25  # <<< cambias solo esto
Z_NAME = f"z_{Z_INDEX:06d}"  

# =============================
# CONFIG
# =============================

DB_PATH = Path("Sets/model.db")
GRAPH_DIR = Path("Graphs/Coverage/Single_Z")
GRAPH_DIR.mkdir(parents=True, exist_ok=True)

SHAPE_PATH = Path("extern data/DPA 2024/COMUNAS/COMUNAS_v1.shp")
CENSUS_PATH = Path("extern data/CENSO/Base_manzana_entidad_CPV24.csv")

DC_RADIUS = 15000
DS_RADIUS = 5000
MDCP_RADIUS = 10000

PERSONS_PER_HOUSEHOLD = 2.8
ZOOM_PADDING_M = 8000

BUFFER_COLORS = [
    "#e41a1c",
    "#ff7f00",
    "#ffff33",
    "#f781bf",
    "#a65628",
    "#ffffff",
    "#00ffff",
    "#ff1493",
    "#ffd700",
    "#8b0000",
    "#00ced1",
    "#ff4500",
    "#f4a460",
    "#fffacd",
    "#dc143c",
    "#20b2aa",
    "#ff69b4",
    "#ffa500",
    "#ffe4b5",
]


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
    gdf["HOUSEHOLDS"] = gdf["POP"] / PERSONS_PER_HOUSEHOLD

    return gdf.to_crs(epsg=32719)


def build_color_map(facility_names):
    color_map = {}
    for i, facility in enumerate(facility_names):
        color_map[facility] = BUFFER_COLORS[i % len(BUFFER_COLORS)]
    return color_map


def get_zoom_bounds(active_facilities, buffers, padding_m=8000):
    if not active_facilities:
        return None

    geom_union = unary_union([buffers[f] for f in active_facilities])
    minx, miny, maxx, maxy = geom_union.bounds

    return (
        minx - padding_m,
        maxx + padding_m,
        miny - padding_m,
        maxy + padding_m,
    )


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

    # Load facilities
    f_df = pd.read_sql("""
        SELECT facility_name, type, latitude, longitude
        FROM F
        ORDER BY facility_name
    """, conn)

    buffers = {}
    projected_xy = {}

    for _, row in f_df.iterrows():
        facility_name = row["facility_name"]
        lon = float(row["longitude"])
        lat = float(row["latitude"])
        facility_type = row["type"]

        radius = get_radius_by_type(facility_type)
        buffers[facility_name] = create_buffer(lon, lat, radius)
        projected_xy[facility_name] = project_point_xy(lon, lat)

    facility_color_map = build_color_map(f_df["facility_name"].tolist())

    # Load geography
    gdf = prepare_geography()

    # Load Z row
    z_df = pd.read_sql("SELECT * FROM Z WHERE z_name = ?", conn, params=[Z_NAME])

    if z_df.empty:
        conn.close()
        raise ValueError(f"z_name not found: {Z_NAME}")

    row = z_df.iloc[0]

    facility_cols = [
        c for c in z_df.columns
        if c not in ("z_name", "covered_population", "covered_households")
    ]

    active = [c for c in facility_cols if int(row[c]) == 1]

    fig, ax = plt.subplots(figsize=(10, 10))

    gdf.plot(
        column="HOUSEHOLDS",
        ax=ax,
        legend=True,
        cmap="viridis",
        linewidth=0.5,
        edgecolor="black"
    )

    legend_handles = []
    legend_labels = []

    for facility in active:
        color = facility_color_map[facility]

        gpd.GeoSeries([buffers[facility]], crs="EPSG:32719").plot(
            ax=ax,
            facecolor=color,
            edgecolor=color,
            alpha=0.22,
            linewidth=2
        )

    for facility in active:
        x, y = projected_xy[facility]
        color = facility_color_map[facility]

        scatter = ax.scatter(x, y, color=color, edgecolors="black", s=90, zorder=5)

        ax.text(
            x,
            y,
            f" {facility}",
            fontsize=8,
            zorder=6,
            bbox=dict(facecolor="white", alpha=0.7, edgecolor="none", pad=1.5)
        )

        legend_handles.append(scatter)
        legend_labels.append(facility)

    bounds = get_zoom_bounds(active, buffers, padding_m=ZOOM_PADDING_M)
    if bounds is not None:
        minx, maxx, miny, maxy = bounds
        ax.set_xlim(minx, maxx)
        ax.set_ylim(miny, maxy)

    ax.set_title(
        f"{Z_NAME}\n"
        f"Covered population: {row['covered_population']:,.0f} | "
        f"Covered households: {row['covered_households']:,.0f}"
    )

    ax.legend(legend_handles, legend_labels, loc="upper left", fontsize=8)

    ax.set_axis_off()
    plt.tight_layout()

    output_path = GRAPH_DIR / f"{Z_NAME}.png"
    plt.savefig(output_path, dpi=300)
    plt.close()

    conn.close()

    print(f"Graph saved to: {output_path}")


if __name__ == "__main__":
    main()