import sqlite3
import random
from pathlib import Path

import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt

from shapely.geometry import Point
from shapely.ops import unary_union

# =============================
# FILE NAME
# =============================
# plot_Z_coverage_graphs.py

# =============================
# CONFIG
# =============================

DB_PATH = Path("Sets/model.db")

GRAPH_DIR = Path("Graphs/Coverage")
GRAPH_DIR.mkdir(parents=True, exist_ok=True)

SHAPE_PATH = Path("extern data/DPA 2024/COMUNAS/COMUNAS_v1.shp")
CENSUS_PATH = Path("extern data/CENSO/Base_manzana_entidad_CPV24.csv")

DC_RADIUS = 15000
DS_RADIUS = 5000
MDCP_RADIUS = 10000

PERSONS_PER_HOUSEHOLD = 2.8

N_RANDOM_PLOTS = 10
SEED = 42
ZOOM_PADDING_M = 8000

# Colors intentionally chosen to contrast with viridis
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
    random.seed(SEED)

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

    # ---- load geography ----
    gdf = prepare_geography()

    # ---- load Z ----
    z_df = pd.read_sql("SELECT * FROM Z", conn)

    if z_df.empty:
        conn.close()
        raise ValueError("Table Z is empty.")

    required_cols = {"z_name", "covered_population", "covered_households"}
    missing_required = required_cols - set(z_df.columns)
    if missing_required:
        conn.close()
        raise ValueError(
            "Table Z is missing required columns: "
            + ", ".join(sorted(missing_required))
            + ". Run update_Z_coverage_columns.py first."
        )

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

    # =============================
    # HISTOGRAM: population
    # =============================
    pop_series = z_df["covered_population"].fillna(0.0)
    pop_mean = float(pop_series.mean())
    pop_std = float(pop_series.std())

    plt.figure(figsize=(10, 6))
    plt.hist(pop_series, bins=50, edgecolor="black")
    plt.axvline(pop_mean, linestyle="--", linewidth=2, label=f"Mean = {pop_mean:,.0f}")
    plt.axvline(
        pop_mean + pop_std,
        linestyle=":",
        linewidth=2,
        label=f"Mean + 1 SD = {pop_mean + pop_std:,.0f}"
    )
    plt.axvline(
        pop_mean - pop_std,
        linestyle=":",
        linewidth=2,
        label=f"Mean - 1 SD = {pop_mean - pop_std:,.0f}"
    )
    plt.title("Distribution of Covered Population")
    plt.xlabel("Covered Population")
    plt.ylabel("Frequency")
    plt.legend()
    plt.tight_layout()
    plt.savefig(GRAPH_DIR / "coverage_histogram_population.png", dpi=300)
    plt.close()

    print("Population histogram saved.")

    # =============================
    # HISTOGRAM: households
    # =============================
    hh_series = z_df["covered_households"].fillna(0.0)
    hh_mean = float(hh_series.mean())
    hh_std = float(hh_series.std())

    plt.figure(figsize=(10, 6))
    plt.hist(hh_series, bins=50, edgecolor="black")
    plt.axvline(hh_mean, linestyle="--", linewidth=2, label=f"Mean = {hh_mean:,.0f}")
    plt.axvline(
        hh_mean + hh_std,
        linestyle=":",
        linewidth=2,
        label=f"Mean + 1 SD = {hh_mean + hh_std:,.0f}"
    )
    plt.axvline(
        hh_mean - hh_std,
        linestyle=":",
        linewidth=2,
        label=f"Mean - 1 SD = {hh_mean - hh_std:,.0f}"
    )
    plt.title("Distribution of Covered Households")
    plt.xlabel("Covered Households")
    plt.ylabel("Frequency")
    plt.legend()
    plt.tight_layout()
    plt.savefig(GRAPH_DIR / "coverage_histogram_households.png", dpi=300)
    plt.close()

    print("Households histogram saved.")

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

            scatter = ax.scatter(
                x,
                y,
                color=color,
                edgecolors="black",
                s=90,
                zorder=5
            )

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
            f"Combination {row['z_name']}\n"
            f"Active facilities: {len(active)} | "
            f"Covered population: {row['covered_population']:,.0f} | "
            f"Covered households: {row['covered_households']:,.0f}"
        )

        ax.legend(
            legend_handles,
            legend_labels,
            title="Active facilities",
            loc="upper left",
            fontsize=8,
            title_fontsize=9,
            frameon=True
        )

        ax.set_axis_off()
        plt.tight_layout()
        plt.savefig(GRAPH_DIR / f"{row['z_name']}.png", dpi=300)
        plt.close()

    conn.close()
    print("Random maps saved.")
    print(f"Graphs saved in: {GRAPH_DIR}")


if __name__ == "__main__":
    main()