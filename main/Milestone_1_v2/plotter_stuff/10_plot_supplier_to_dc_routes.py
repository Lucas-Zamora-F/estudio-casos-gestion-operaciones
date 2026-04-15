from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Dict, Any

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
import requests
from shapely.geometry import box, shape
from matplotlib.patches import Rectangle


# ============================================================
# CONFIG
# ============================================================
SHAPEFILE_PATH = Path(r"extern data/DPA 2024/COMUNAS/COMUNAS_v1.shp")

DPI = 220

POINT_SIZE_SUPPLIER = 170
POINT_SIZE_DC = 120

TITLE_FONT = 15
SUBTITLE_FONT = 11
LEGEND_FONT = 8
ANNOTATION_FONT = 8

MAP_PADDING_RATIO_X = 0.12
MAP_PADDING_RATIO_Y = 0.12

ORS_API_KEY = os.getenv("ORS_API_KEY", "")
ORS_BASE_URL = "https://api.openrouteservice.org"
ORS_PROFILE = "driving-hgv"

ROUTE_SLEEP_SECONDS = 1.2

SUMMARY_MAX_ROWS_SINGLE_PAGE = 12

ROUTE_COLOR_E = "blue"
ROUTE_COLOR_WM = "green"
ROUTE_COLOR_SCL = "orange"


# ============================================================
# LOGGING
# ============================================================
def log(msg: str) -> None:
    now = time.strftime("%H:%M:%S")
    print(f"[{now}] {msg}")


def section(title: str) -> None:
    print("\n" + "=" * 90)
    print(title)
    print("=" * 90)


# ============================================================
# BASIC HELPERS
# ============================================================
def validate_api_key() -> None:
    if not ORS_API_KEY:
        raise ValueError("Debes definir ORS_API_KEY como variable de entorno.")


def build_headers() -> dict[str, str]:
    return {
        "Authorization": ORS_API_KEY,
        "Content-Type": "application/json; charset=utf-8",
        "Accept": "application/json, application/geo+json",
    }


def safe_float(x, default: float = 0.0) -> float:
    try:
        if pd.isna(x):
            return default
        return float(x)
    except Exception:
        return default


def normalize_text(x: Any) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip()


def is_same_location(
    lat1: float,
    lon1: float,
    lat2: float,
    lon2: float,
    tol: float = 1e-6,
) -> bool:
    return abs(lat1 - lat2) < tol and abs(lon1 - lon2) < tol


def load_communes_gdf(shapefile_path: Path = SHAPEFILE_PATH) -> gpd.GeoDataFrame:
    if not shapefile_path.exists():
        raise FileNotFoundError(f"No existe el shapefile: {shapefile_path.resolve()}")

    gdf = gpd.read_file(shapefile_path)

    if gdf.crs is None:
        raise ValueError("El shapefile de comunas no tiene CRS definido.")

    return gdf.to_crs(epsg=4326)


def format_number(x: Any, decimals: int = 2) -> str:
    return f"{safe_float(x):,.{decimals}f}"


def get_color_by_origin_category(category: str) -> str:
    category = normalize_text(category)

    if category == "E":
        return ROUTE_COLOR_E
    if category == "WM":
        return ROUTE_COLOR_WM
    if category == "S_cl":
        return ROUTE_COLOR_SCL
    return "gray"


def get_route_label(flow_type: str, origin_category: str) -> str:
    flow_type = normalize_text(flow_type)
    origin_category = normalize_text(origin_category)

    if flow_type == "E_to_DC" or origin_category == "E":
        return "E -> DC"
    if flow_type == "WM_to_DC" or origin_category == "WM":
        return "WM -> DC"
    if flow_type in {"Scl_to_DC", "S_cl_to_DC"} or origin_category == "S_cl":
        return "S_cl -> DC"
    return "Other"


# ============================================================
# ORS DIRECTIONS
# ============================================================
def call_directions_api(
    origin_lon: float,
    origin_lat: float,
    destination_lon: float,
    destination_lat: float,
    profile: str = ORS_PROFILE,
):
    url = f"{ORS_BASE_URL}/v2/directions/{profile}/geojson"

    payload = {
        "coordinates": [
            [float(origin_lon), float(origin_lat)],
            [float(destination_lon), float(destination_lat)],
        ],
        "radiuses": [-1, -1],
        "instructions": False,
        "geometry_simplify": False,
    }

    response = requests.post(
        url,
        headers=build_headers(),
        json=payload,
        timeout=120,
    )

    if response.status_code != 200:
        raise RuntimeError(
            f"Directions API error {response.status_code}: {response.text}"
        )

    data = response.json()
    features = data.get("features", [])

    if not features:
        return None

    geometry_dict = features[0].get("geometry")
    if not geometry_dict:
        return None

    return shape(geometry_dict)


def call_directions_api_with_retry(
    origin_lon: float,
    origin_lat: float,
    destination_lon: float,
    destination_lat: float,
    profile: str = ORS_PROFILE,
    max_retries: int = 4,
):
    for attempt in range(max_retries):
        try:
            return call_directions_api(
                origin_lon=origin_lon,
                origin_lat=origin_lat,
                destination_lon=destination_lon,
                destination_lat=destination_lat,
                profile=profile,
            )
        except Exception as exc:
            error_text = str(exc)

            if "429" in error_text or "Rate Limit Exceeded" in error_text:
                wait_time = 2 * (attempt + 1)
                time.sleep(wait_time)
                continue

            raise

    return None


# ============================================================
# DATA PREP
# ============================================================
def build_routes_df(df_connections_month: pd.DataFrame) -> pd.DataFrame:
    if df_connections_month.empty:
        return pd.DataFrame(
            columns=[
                "origin",
                "destination",
                "lat_o",
                "lon_o",
                "lat_d",
                "lon_d",
                "flow_type",
                "origin_category",
                "destination_type",
            ]
        )

    df = df_connections_month.copy()

    routes_df = pd.DataFrame({
        "origin": df["origin"],
        "destination": df["destination"],
        "lat_o": pd.to_numeric(df["origin_lat"], errors="coerce"),
        "lon_o": pd.to_numeric(df["origin_lon"], errors="coerce"),
        "lat_d": pd.to_numeric(df["destination_lat"], errors="coerce"),
        "lon_d": pd.to_numeric(df["destination_lon"], errors="coerce"),
        "flow_type": df.get("flow_type", ""),
        "origin_category": df.get("origin_category", ""),
        "destination_type": df.get("destination_type", ""),
    })

    routes_df = routes_df.dropna(
        subset=["lat_o", "lon_o", "lat_d", "lon_d"]
    ).reset_index(drop=True)

    return routes_df


def filter_all_supplier_to_dc(routes_df: pd.DataFrame) -> pd.DataFrame:
    if routes_df.empty:
        return routes_df

    df = routes_df.copy()

    if "destination_type" in df.columns:
        df = df[df["destination_type"] == "DC"].copy()

    return df.reset_index(drop=True)


def build_origins_destinations_from_routes(
    routes_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if routes_df.empty:
        empty = pd.DataFrame(columns=["point_name", "latitude", "longitude"])
        return empty, empty

    origins = (
        routes_df[["origin", "lat_o", "lon_o"]]
        .drop_duplicates()
        .rename(columns={
            "origin": "point_name",
            "lat_o": "latitude",
            "lon_o": "longitude",
        })
        .reset_index(drop=True)
    )

    destinations = (
        routes_df[["destination", "lat_d", "lon_d"]]
        .drop_duplicates()
        .rename(columns={
            "destination": "point_name",
            "lat_d": "latitude",
            "lon_d": "longitude",
        })
        .reset_index(drop=True)
    )

    return origins, destinations


def filter_supplier_to_dc_summary(summary_df: pd.DataFrame) -> pd.DataFrame:
    if summary_df.empty:
        return summary_df

    df = summary_df.copy()

    if "destination_type" in df.columns:
        df = df[df["destination_type"] == "DC"].copy()

    return df.reset_index(drop=True)


def prepare_summary_table_df(df_summary_month: pd.DataFrame) -> pd.DataFrame:
    if df_summary_month.empty:
        return pd.DataFrame()

    df = filter_supplier_to_dc_summary(df_summary_month).copy()

    if df.empty:
        return df

    numeric_cols = [
        "quantity_kg",
        "purchase_cost_usd",
        "transport_cost_usd",
        "total_landed_cost_usd",
        "unit_landed_cost_usd_per_kg",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    text_cols = [
        "origin",
        "supplier_type",
        "origin_country",
        "product",
        "destination",
        "flow_type",
        "origin_category",
    ]
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].apply(normalize_text)

    df["route_label"] = df.apply(
        lambda r: get_route_label(
            flow_type=r.get("flow_type", ""),
            origin_category=r.get("origin_category", ""),
        ),
        axis=1,
    )

    df["route_color"] = df.apply(
        lambda r: get_color_by_origin_category(r.get("origin_category", "")),
        axis=1,
    )

    preferred_cols = [
        "route_label",
        "route_color",
        "origin",
        "origin_country",
        "product",
        "destination",
        "quantity_kg",
        "purchase_cost_usd",
        "transport_cost_usd",
        "total_landed_cost_usd",
        "unit_landed_cost_usd_per_kg",
    ]
    for col in preferred_cols:
        if col not in df.columns:
            df[col] = ""

    df = df[preferred_cols].copy()

    df = df.sort_values(
        by=["route_label", "origin", "destination", "product"]
    ).reset_index(drop=True)

    return df


def split_dataframe_into_chunks(df: pd.DataFrame, chunk_size: int) -> list[pd.DataFrame]:
    if df.empty:
        return []

    chunks: list[pd.DataFrame] = []
    for start in range(0, len(df), chunk_size):
        chunks.append(df.iloc[start:start + chunk_size].copy())
    return chunks


def build_display_table(df: pd.DataFrame) -> pd.DataFrame:
    display_df = pd.DataFrame({
        "Route": df["route_label"].astype(str),
        "Supplier": df["origin"].astype(str),
        "Country": df["origin_country"].astype(str),
        "Product": df["product"].astype(str),
        "DC": df["destination"].astype(str),
        "Qty (kg)": df["quantity_kg"].apply(lambda x: format_number(x, 1)),
        "Purchase (USD)": df["purchase_cost_usd"].apply(lambda x: format_number(x, 2)),
        "Transport (USD)": df["transport_cost_usd"].apply(lambda x: format_number(x, 2)),
        "Total (USD)": df["total_landed_cost_usd"].apply(lambda x: format_number(x, 2)),
        "USD/kg": df["unit_landed_cost_usd_per_kg"].apply(lambda x: format_number(x, 4)),
    })

    return display_df


# ============================================================
# ROUTE GEOMETRIES
# ============================================================
def compute_route_geometries_from_routes_df(
    routes_df: pd.DataFrame,
    profile: str = ORS_PROFILE,
    sleep_seconds: float = ROUTE_SLEEP_SECONDS,
) -> list[dict]:
    route_records: list[dict] = []

    if routes_df.empty:
        return route_records

    for _, row in routes_df.iterrows():
        origin_name = row["origin"]
        destination_name = row["destination"]
        origin_lat = safe_float(row["lat_o"])
        origin_lon = safe_float(row["lon_o"])
        destination_lat = safe_float(row["lat_d"])
        destination_lon = safe_float(row["lon_d"])
        origin_category = row.get("origin_category", "")

        if is_same_location(origin_lat, origin_lon, destination_lat, destination_lon):
            continue

        try:
            geometry = call_directions_api_with_retry(
                origin_lon=origin_lon,
                origin_lat=origin_lat,
                destination_lon=destination_lon,
                destination_lat=destination_lat,
                profile=profile,
                max_retries=4,
            )
        except Exception:
            geometry = None

        if geometry is not None:
            route_records.append(
                {
                    "origin_name": origin_name,
                    "destination_name": destination_name,
                    "route_name": f"{origin_name} -> {destination_name}",
                    "origin_category": origin_category,
                    "geometry": geometry,
                }
            )

        time.sleep(sleep_seconds)

    return route_records


def build_route_gdf(route_records: list[dict]) -> gpd.GeoDataFrame:
    if not route_records:
        return gpd.GeoDataFrame(
            columns=["origin_name", "destination_name", "route_name", "origin_category", "geometry"],
            geometry="geometry",
            crs="EPSG:4326",
        )

    return gpd.GeoDataFrame(route_records, geometry="geometry", crs="EPSG:4326")


# ============================================================
# MAP HELPERS
# ============================================================
def get_bounds(
    origins: pd.DataFrame,
    destinations: pd.DataFrame,
    route_gdf: gpd.GeoDataFrame,
) -> tuple[float, float, float, float]:
    lon_values = origins["longitude"].tolist() + destinations["longitude"].tolist()
    lat_values = origins["latitude"].tolist() + destinations["latitude"].tolist()

    if not route_gdf.empty:
        minx, miny, maxx, maxy = route_gdf.total_bounds
        lon_values.extend([minx, maxx])
        lat_values.extend([miny, maxy])

    min_lon = min(lon_values)
    max_lon = max(lon_values)
    min_lat = min(lat_values)
    max_lat = max(lat_values)

    dx = max(max_lon - min_lon, 0.20)
    dy = max(max_lat - min_lat, 0.20)

    min_aspect_ratio = 0.6
    current_ratio = dy / dx if dx > 0 else 1.0

    if current_ratio < min_aspect_ratio:
        target_dy = dx * min_aspect_ratio
        extra = (target_dy - dy) / 2
        min_lat -= extra
        max_lat += extra
        dy = target_dy

    pad_x = dx * MAP_PADDING_RATIO_X
    pad_y = dy * MAP_PADDING_RATIO_Y

    return (
        min_lon - pad_x,
        max_lon + pad_x,
        min_lat - pad_y,
        max_lat + pad_y,
    )


def select_map_window(
    communes_gdf: gpd.GeoDataFrame,
    xmin: float,
    xmax: float,
    ymin: float,
    ymax: float,
) -> gpd.GeoDataFrame:
    bbox_geom = box(xmin, ymin, xmax, ymax)
    sub = communes_gdf.loc[communes_gdf.intersects(bbox_geom)].copy()

    if sub.empty:
        return communes_gdf.copy()

    return sub


# ============================================================
# DRAW MAP ON AXIS
# ============================================================
def draw_routes_map_on_axis(
    ax,
    routes_df: pd.DataFrame,
    communes_gdf: gpd.GeoDataFrame,
    map_title: str,
) -> bool:
    routes_df = filter_all_supplier_to_dc(routes_df)

    if routes_df.empty:
        ax.axis("off")
        ax.text(0.5, 0.5, "No supplier-to-DC routes for this month.", ha="center", va="center")
        return False

    origins, destinations = build_origins_destinations_from_routes(routes_df)

    route_records = compute_route_geometries_from_routes_df(
        routes_df=routes_df,
        profile=ORS_PROFILE,
        sleep_seconds=ROUTE_SLEEP_SECONDS,
    )
    route_gdf = build_route_gdf(route_records)

    if origins.empty or destinations.empty:
        ax.axis("off")
        ax.text(0.5, 0.5, "No valid route points to plot.", ha="center", va="center")
        return False

    xmin, xmax, ymin, ymax = get_bounds(
        origins=origins,
        destinations=destinations,
        route_gdf=route_gdf,
    )

    communes_plot = select_map_window(
        communes_gdf=communes_gdf,
        xmin=xmin,
        xmax=xmax,
        ymin=ymin,
        ymax=ymax,
    )

    ax.set_facecolor("#cfe8ff")

    if not communes_plot.empty:
        communes_plot.plot(
            ax=ax,
            facecolor="white",
            edgecolor="gray",
            linewidth=0.4,
            zorder=1,
        )

    if not route_gdf.empty:
        for _, row in route_gdf.iterrows():
            color = get_color_by_origin_category(str(row.get("origin_category", "")))
            gpd.GeoSeries([row.geometry], crs="EPSG:4326").plot(
                ax=ax,
                linewidth=2.5,
                color=color,
                zorder=3,
            )

    for _, row in origins.iterrows():
        ax.scatter(
            row["longitude"],
            row["latitude"],
            s=POINT_SIZE_SUPPLIER,
            marker="o",
            color="black",
            edgecolors="black",
            linewidths=0.8,
            zorder=4,
        )
        ax.text(
            row["longitude"],
            row["latitude"],
            row["point_name"],
            fontsize=ANNOTATION_FONT,
            fontweight="bold",
            zorder=5,
        )

    ax.scatter(
        destinations["longitude"],
        destinations["latitude"],
        s=POINT_SIZE_DC,
        marker="^",
        color="red",
        edgecolors="black",
        linewidths=0.8,
        zorder=4,
        label="DCs",
    )

    for _, row in destinations.iterrows():
        ax.text(
            row["longitude"],
            row["latitude"],
            row["point_name"],
            fontsize=ANNOTATION_FONT,
            zorder=5,
        )

    legend_handles = [
        plt.Line2D([0], [0], color=ROUTE_COLOR_E, lw=2.5, label="E -> DC"),
        plt.Line2D([0], [0], color=ROUTE_COLOR_WM, lw=2.5, label="WM -> DC"),
        plt.Line2D([0], [0], color=ROUTE_COLOR_SCL, lw=2.5, label="S_cl -> DC"),
        plt.Line2D(
            [0],
            [0],
            marker="^",
            color="w",
            markerfacecolor="red",
            markeredgecolor="black",
            markersize=10,
            linestyle="None",
            label="DCs",
        ),
    ]

    ax.set_xlim(xmin, xmax)
    ax.set_ylim(ymin, ymax)
    ax.set_aspect("equal", adjustable="box")
    ax.set_title(map_title, fontsize=SUBTITLE_FONT)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.legend(handles=legend_handles, loc="best", fontsize=LEGEND_FONT)

    return True


# ============================================================
# SINGLE MAP PNG
# ============================================================
def plot_supplier_to_dc_routes_for_month(
    model_name: str,
    month: str,
    month_dir: Path,
    routes_df: pd.DataFrame,
    communes_gdf: gpd.GeoDataFrame,
) -> None:
    routes_df = filter_all_supplier_to_dc(routes_df)

    if routes_df.empty:
        log(f"    [INFO] {model_name} | {month}: no hay rutas supplier_to_DC.")
        return

    origins, destinations = build_origins_destinations_from_routes(routes_df)

    route_records = compute_route_geometries_from_routes_df(
        routes_df=routes_df,
        profile=ORS_PROFILE,
        sleep_seconds=ROUTE_SLEEP_SECONDS,
    )
    route_gdf = build_route_gdf(route_records)

    if origins.empty or destinations.empty:
        log(f"    [INFO] {model_name} | {month}: no hay puntos válidos para graficar.")
        return

    xmin, xmax, ymin, ymax = get_bounds(
        origins=origins,
        destinations=destinations,
        route_gdf=route_gdf,
    )

    communes_plot = select_map_window(
        communes_gdf=communes_gdf,
        xmin=xmin,
        xmax=xmax,
        ymin=ymin,
        ymax=ymax,
    )

    width = max(xmax - xmin, 0.20)
    height = max(ymax - ymin, 0.20)
    aspect_ratio = height / width if width > 0 else 1.0

    fig_width = 13
    fig_height = max(7, min(13, fig_width * aspect_ratio))

    fig, ax = plt.subplots(figsize=(fig_width, fig_height))
    ax.set_facecolor("#cfe8ff")

    if not communes_plot.empty:
        communes_plot.plot(
            ax=ax,
            facecolor="white",
            edgecolor="gray",
            linewidth=0.4,
            zorder=1,
        )

    if not route_gdf.empty:
        for _, row in route_gdf.iterrows():
            color = get_color_by_origin_category(str(row.get("origin_category", "")))
            gpd.GeoSeries([row.geometry], crs="EPSG:4326").plot(
                ax=ax,
                linewidth=2.5,
                color=color,
                zorder=3,
            )

    for _, row in origins.iterrows():
        ax.scatter(
            row["longitude"],
            row["latitude"],
            s=POINT_SIZE_SUPPLIER,
            marker="o",
            color="black",
            edgecolors="black",
            linewidths=0.8,
            zorder=4,
        )
        ax.text(
            row["longitude"],
            row["latitude"],
            row["point_name"],
            fontsize=ANNOTATION_FONT,
            fontweight="bold",
            zorder=5,
        )

    ax.scatter(
        destinations["longitude"],
        destinations["latitude"],
        s=POINT_SIZE_DC,
        marker="^",
        color="red",
        edgecolors="black",
        linewidths=0.8,
        zorder=4,
        label="DCs",
    )

    for _, row in destinations.iterrows():
        ax.text(
            row["longitude"],
            row["latitude"],
            row["point_name"],
            fontsize=ANNOTATION_FONT,
            zorder=5,
        )

    legend_handles = [
        plt.Line2D([0], [0], color=ROUTE_COLOR_E, lw=2.5, label="E -> DC"),
        plt.Line2D([0], [0], color=ROUTE_COLOR_WM, lw=2.5, label="WM -> DC"),
        plt.Line2D([0], [0], color=ROUTE_COLOR_SCL, lw=2.5, label="S_cl -> DC"),
        plt.Line2D(
            [0],
            [0],
            marker="^",
            color="w",
            markerfacecolor="red",
            markeredgecolor="black",
            markersize=10,
            linestyle="None",
            label="DCs",
        ),
    ]

    ax.set_xlim(xmin, xmax)
    ax.set_ylim(ymin, ymax)
    ax.set_aspect("equal", adjustable="box")
    ax.set_title(
        f"{model_name} | {month} | Supplier to DC heavy-truck routes",
        fontsize=TITLE_FONT,
    )
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.legend(handles=legend_handles, loc="best", fontsize=LEGEND_FONT)

    month_dir.mkdir(parents=True, exist_ok=True)
    out_path = month_dir / "supplier_to_dc_routes.png"

    fig.savefig(out_path, dpi=DPI, bbox_inches="tight")
    plt.close(fig)

    log(f"    [OK] Guardado: {out_path}")


# ============================================================
# SINGLE SUMMARY PNG
# ============================================================
def plot_supplier_to_dc_summary_for_month(
    model_name: str,
    month: str,
    month_dir: Path,
    df_summary_month: pd.DataFrame,
) -> None:
    summary_df = prepare_summary_table_df(df_summary_month)

    if summary_df.empty:
        log(f"    [INFO] {model_name} | {month}: no hay summary supplier_to_DC.")
        return

    month_dir.mkdir(parents=True, exist_ok=True)

    display_df = build_display_table(summary_df)

    fig_height = max(6.8, 1.8 + len(display_df) * 0.52)
    fig, ax = plt.subplots(figsize=(18, fig_height))
    ax.axis("off")

    ax.text(
        0.5,
        1.02,
        f"{model_name} | {month} | Supplier-to-DC sourcing summary",
        ha="center",
        va="bottom",
        fontsize=TITLE_FONT,
        fontweight="bold",
        transform=ax.transAxes,
    )

    col_widths = [0.06, 0.12, 0.08, 0.13, 0.09, 0.10, 0.12, 0.11, 0.11, 0.08]

    table = ax.table(
        cellText=display_df.values,
        colLabels=display_df.columns,
        loc="upper center",
        cellLoc="left",
        colLoc="center",
        colWidths=col_widths,
        bbox=[0.0, 0.13, 1.0, 0.80],
    )

    table.auto_set_font_size(False)
    table.set_fontsize(8)

    for (row, col), cell in table.get_celld().items():
        cell.set_linewidth(0.35)

        if row == 0:
            cell.set_text_props(weight="bold")
            cell.set_facecolor("#d0d0d0")
            cell.set_height(cell.get_height() * 1.15)
        else:
            if row % 2 == 0:
                cell.set_facecolor("#f6f6f6")
            else:
                cell.set_facecolor("white")

            if col >= 5:
                cell.get_text().set_ha("right")
            else:
                cell.get_text().set_ha("left")

    for row_idx in range(1, len(display_df) + 1):
        route_color = summary_df.iloc[row_idx - 1]["route_color"]
        route_cell = table[(row_idx, 0)]
        route_cell.set_facecolor(route_color)
        route_cell.get_text().set_color("white")
        route_cell.get_text().set_weight("bold")
        route_cell.get_text().set_ha("center")

    total_qty = safe_float(summary_df["quantity_kg"].sum())
    total_purchase = safe_float(summary_df["purchase_cost_usd"].sum())
    total_transport = safe_float(summary_df["transport_cost_usd"].sum())
    total_landed = safe_float(summary_df["total_landed_cost_usd"].sum())

    totals_text = (
        f"Monthly totals  |  Qty: {format_number(total_qty, 1)} kg"
        f"  |  Purchase: USD {format_number(total_purchase, 2)}"
        f"  |  Transport: USD {format_number(total_transport, 2)}"
        f"  |  Total: USD {format_number(total_landed, 2)}"
    )

    ax.text(
        0.0,
        0.04,
        totals_text,
        ha="left",
        va="bottom",
        fontsize=10,
        transform=ax.transAxes,
    )

    ax.text(
        0.0,
        0.00,
        "Total cost = Purchase Cost + Transport Cost",
        ha="left",
        va="bottom",
        fontsize=9,
        transform=ax.transAxes,
    )

    out_path = month_dir / "supplier_to_dc_summary.png"
    fig.savefig(out_path, dpi=DPI, bbox_inches="tight")
    plt.close(fig)

    log(f"    [OK] Guardado: {out_path}")


# ============================================================
# COMBINED PNG
# ============================================================
def plot_supplier_to_dc_combined_for_month(
    model_name: str,
    month: str,
    month_dir: Path,
    routes_df: pd.DataFrame,
    df_summary_month: pd.DataFrame,
    communes_gdf: gpd.GeoDataFrame,
) -> None:
    routes_df = filter_all_supplier_to_dc(routes_df)
    summary_df = prepare_summary_table_df(df_summary_month)

    if routes_df.empty and summary_df.empty:
        log(f"    [INFO] {model_name} | {month}: no hay data para combined.")
        return

    if len(summary_df) > SUMMARY_MAX_ROWS_SINGLE_PAGE:
        log(
            f"    [INFO] {model_name} | {month}: summary con {len(summary_df)} filas; "
            f"se omite combined PNG para evitar tabla ilegible."
        )
        return

    month_dir.mkdir(parents=True, exist_ok=True)

    fig = plt.figure(figsize=(18, 14))
    gs = fig.add_gridspec(
        nrows=2,
        ncols=1,
        height_ratios=[1.15, 1.55],
        hspace=0.15,
    )

    ax_table = fig.add_subplot(gs[0])
    ax_map = fig.add_subplot(gs[1])

    fig.suptitle(
        f"{model_name} | {month} | Supplier-to-DC sourcing overview",
        fontsize=TITLE_FONT,
        fontweight="bold",
        y=0.98,
    )

    # ---------------------------
    # TABLE
    # ---------------------------
    ax_table.axis("off")
    ax_table.text(
        0.0,
        1.03,
        "Inbound sourcing summary",
        ha="left",
        va="bottom",
        fontsize=SUBTITLE_FONT,
        fontweight="bold",
        transform=ax_table.transAxes,
    )

    if summary_df.empty:
        ax_table.text(
            0.5,
            0.5,
            "No supplier-to-DC summary data for this month.",
            ha="center",
            va="center",
            fontsize=10,
            transform=ax_table.transAxes,
        )
    else:
        display_df = build_display_table(summary_df)

        col_widths = [0.06, 0.12, 0.08, 0.13, 0.09, 0.10, 0.12, 0.11, 0.11, 0.08]

        table = ax_table.table(
            cellText=display_df.values,
            colLabels=display_df.columns,
            loc="upper center",
            cellLoc="left",
            colLoc="center",
            colWidths=col_widths,
            bbox=[0.0, 0.12, 1.0, 0.82],
        )

        table.auto_set_font_size(False)
        table.set_fontsize(8)

        for (row, col), cell in table.get_celld().items():
            cell.set_linewidth(0.35)

            if row == 0:
                cell.set_text_props(weight="bold")
                cell.set_facecolor("#d0d0d0")
                cell.set_height(cell.get_height() * 1.15)
            else:
                if row % 2 == 0:
                    cell.set_facecolor("#f6f6f6")
                else:
                    cell.set_facecolor("white")

                if col >= 5:
                    cell.get_text().set_ha("right")
                else:
                    cell.get_text().set_ha("left")

        for row_idx in range(1, len(display_df) + 1):
            route_color = summary_df.iloc[row_idx - 1]["route_color"]
            route_cell = table[(row_idx, 0)]
            route_cell.set_facecolor(route_color)
            route_cell.get_text().set_color("white")
            route_cell.get_text().set_weight("bold")
            route_cell.get_text().set_ha("center")

        total_qty = safe_float(summary_df["quantity_kg"].sum())
        total_purchase = safe_float(summary_df["purchase_cost_usd"].sum())
        total_transport = safe_float(summary_df["transport_cost_usd"].sum())
        total_landed = safe_float(summary_df["total_landed_cost_usd"].sum())

        totals_text = (
            f"Monthly totals  |  Qty: {format_number(total_qty, 1)} kg"
            f"  |  Purchase: USD {format_number(total_purchase, 2)}"
            f"  |  Transport: USD {format_number(total_transport, 2)}"
            f"  |  Total: USD {format_number(total_landed, 2)}"
        )

        ax_table.text(
            0.0,
            0.04,
            totals_text,
            ha="left",
            va="bottom",
            fontsize=10,
            transform=ax_table.transAxes,
        )

        ax_table.text(
            0.0,
            0.00,
            "Total cost = Purchase Cost + Transport Cost",
            ha="left",
            va="bottom",
            fontsize=9,
            transform=ax_table.transAxes,
        )

    # ---------------------------
    # MAP
    # ---------------------------
    draw_routes_map_on_axis(
        ax=ax_map,
        routes_df=routes_df,
        communes_gdf=communes_gdf,
        map_title="Heavy-truck inbound routes",
    )

    out_path = month_dir / "supplier_to_dc_overview.png"
    fig.savefig(out_path, dpi=DPI, bbox_inches="tight")
    plt.close(fig)

    log(f"    [OK] Guardado: {out_path}")


# ============================================================
# MODEL RUN
# ============================================================
def run_one_model(
    model_name: str,
    model_data: Dict[str, Any],
    communes_gdf: gpd.GeoDataFrame,
) -> None:
    log(f"[PROCESS] {model_name}")

    months = model_data.get("months", [])
    graph_dir = Path(model_data["graph_dir"]) if "graph_dir" in model_data else None

    if graph_dir is None:
        raise KeyError(
            f"El modelo {model_name} no contiene 'graph_dir'. "
            f"Plotter.py debe inyectarlo al pasar plot_data al pipeline."
        )

    supplier_to_dc = model_data["supplier_to_dc"]
    df_connections = supplier_to_dc.get("connections", pd.DataFrame())
    df_summary = supplier_to_dc.get("summary", pd.DataFrame())

    if df_connections.empty and df_summary.empty:
        log(f"    [INFO] {model_name}: no hay data supplier_to_dc.")
        return

    for month in months:
        month_dir = graph_dir / month

        df_connections_month = pd.DataFrame()
        if not df_connections.empty and "month" in df_connections.columns:
            df_connections_month = df_connections[df_connections["month"] == month].copy()

        df_summary_month = pd.DataFrame()
        if not df_summary.empty and "month" in df_summary.columns:
            df_summary_month = df_summary[df_summary["month"] == month].copy()

        if not df_connections_month.empty:
            routes_df = build_routes_df(df_connections_month)

            plot_supplier_to_dc_routes_for_month(
                model_name=model_name,
                month=month,
                month_dir=month_dir,
                routes_df=routes_df,
                communes_gdf=communes_gdf,
            )
        else:
            log(f"    [INFO] {model_name} | {month}: no hay connections para mapa.")
            routes_df = pd.DataFrame()

        if not df_summary_month.empty:
            plot_supplier_to_dc_summary_for_month(
                model_name=model_name,
                month=month,
                month_dir=month_dir,
                df_summary_month=df_summary_month,
            )
        else:
            log(f"    [INFO] {model_name} | {month}: no hay summary para tabla.")

        plot_supplier_to_dc_combined_for_month(
            model_name=model_name,
            month=month,
            month_dir=month_dir,
            routes_df=routes_df,
            df_summary_month=df_summary_month,
            communes_gdf=communes_gdf,
        )


# ============================================================
# PUBLIC ENTRYPOINT
# ============================================================
def run_plot_supplier_to_dc_routes(plot_data: Dict[str, Any]) -> None:
    section("PLOT SUPPLIER TO DC ROUTES")

    validate_api_key()

    if "models" not in plot_data:
        raise KeyError("plot_data no contiene la clave 'models'.")

    communes_gdf = load_communes_gdf(SHAPEFILE_PATH)

    for model_name, model_data in plot_data["models"].items():
        run_one_model(
            model_name=model_name,
            model_data=model_data,
            communes_gdf=communes_gdf,
        )

    section("PLOT SUPPLIER TO DC ROUTES COMPLETE")


# ============================================================
# MAIN
# ============================================================
def main() -> None:
    raise RuntimeError(
        "Este script está pensado para ser llamado desde Plotter.py "
        "pasándole plot_data ya construido."
    )


if __name__ == "__main__":
    main()