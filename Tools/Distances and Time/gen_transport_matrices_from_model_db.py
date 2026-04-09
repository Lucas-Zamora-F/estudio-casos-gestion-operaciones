from __future__ import annotations

import sqlite3
import time
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
import requests
from shapely.geometry import shape

# ============================================================
# FILE NAME: gen_transport_matrices_from_model_db.py
# ============================================================

ORS_API_KEY = "eyJvcmciOiI1YjNjZTM1OTc4NTExMTAwMDFjZjYyNDgiLCJpZCI6Ijc0OGZiYTE4ZTQ2MDQ2YjM4YmEwYzFiYzQwOWVjYzJmIiwiaCI6Im11cm11cjY0In0="
ORS_BASE_URL = "https://api.openrouteservice.org"
ORS_PROFILE = "driving-hgv"

RELATIONS = {
    "E_DC": {
        "origin": {
            "source": "E",
            "name_col": "international entry point",
            "type_filter": None,
        },
        "destination": {
            "source": "F",
            "name_col": "facility_name",
            "type_filter": ["DC"],
        },
        "plot_region": "central_chile",
    },
    "WM_CD": {
        "origin": {
            "source": "WM",
            "name_col": "wholesale_market",
            "type_filter": None,
        },
        "destination": {
            "source": "F",
            "name_col": "facility_name",
            "type_filter": ["DC"],
        },
        "plot_region": "santiago",
    },
    "DC_SD": {
    "origin": {
        "source": "F",
        "name_col": "facility_name",
        "type_filter": ["DC"],
    },
    "destination": {
        "source": "F",
        "name_col": "facility_name",
        "type_filter": ["DS"],
    },
    "plot_region": "santiago",
    },
    "DC_MDCP": {
        "origin": {
            "source": "F",
            "name_col": "facility_name",
            "type_filter": ["DC"],
        },
        "destination": {
            "source": "F",
            "name_col": "facility_name",
            "type_filter": ["MDCP"],
        },
        "plot_region": "santiago",
    },
    "S_cl_DC": {
        "origin": {
            "source": "S_cl",
            "name_col": "origin",
            "type_filter": None,
        },
        "destination": {
            "source": "F",
            "name_col": "facility_name",
            "type_filter": ["DC"],
        },
        "plot_region": "central_chile",
    },
}


# ============================================================
# BASIC HELPERS
# ============================================================

def validate_api_key() -> None:
    if not ORS_API_KEY or ORS_API_KEY == "YOUR_ORS_API_KEY_HERE":
        raise ValueError("You must set ORS_API_KEY with your openrouteservice API key.")


def quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def build_headers() -> dict[str, str]:
    return {
        "Authorization": ORS_API_KEY,
        "Content-Type": "application/json; charset=utf-8",
        "Accept": "application/json, application/geo+json",
    }


def is_same_location(
    lat1: float,
    lon1: float,
    lat2: float,
    lon2: float,
    tol: float = 1e-6,
) -> bool:
    return abs(lat1 - lat2) < tol and abs(lon1 - lon2) < tol


# ============================================================
# DATABASE READ
# ============================================================

def load_points_from_table(
    conn: sqlite3.Connection,
    table_name: str,
    name_col: str,
    type_filter: list[str] | None = None,
) -> pd.DataFrame:
    if table_name == "F":
        sql = f"""
            SELECT
                {quote_identifier(name_col)} AS point_name,
                type,
                latitude,
                longitude
            FROM {quote_identifier(table_name)}
        """
        df = pd.read_sql(sql, conn)

        if type_filter is not None:
            df = df[df["type"].isin(type_filter)].copy()
    else:
        sql = f"""
            SELECT
                {quote_identifier(name_col)} AS point_name,
                latitude,
                longitude
            FROM {quote_identifier(table_name)}
        """
        df = pd.read_sql(sql, conn)
        df["type"] = None

    required = {"point_name", "type", "latitude", "longitude"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"Missing required columns in table {table_name}: {sorted(missing)}"
        )

    df["latitude"] = pd.to_numeric(df["latitude"], errors="raise")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="raise")

    if df.empty:
        raise ValueError(
            f"No rows found in table={table_name} with type_filter={type_filter}"
        )

    return df[["point_name", "type", "latitude", "longitude"]].reset_index(drop=True)


# ============================================================
# ORS MATRIX
# ============================================================

def call_matrix_api(
    origins: pd.DataFrame,
    destinations: pd.DataFrame,
    profile: str = ORS_PROFILE,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    url = f"{ORS_BASE_URL}/v2/matrix/{profile}"

    locations = []

    for _, row in origins.iterrows():
        locations.append([float(row["longitude"]), float(row["latitude"])])

    for _, row in destinations.iterrows():
        locations.append([float(row["longitude"]), float(row["latitude"])])

    n_orig = len(origins)
    n_dest = len(destinations)

    payload = {
        "locations": locations,
        "sources": list(range(n_orig)),
        "destinations": list(range(n_orig, n_orig + n_dest)),
        "metrics": ["distance", "duration"],
        "units": "km",
    }

    response = requests.post(
        url,
        headers=build_headers(),
        json=payload,
        timeout=120,
    )

    if response.status_code != 200:
        raise RuntimeError(f"Matrix API error {response.status_code}: {response.text}")

    data = response.json()

    if "distances" not in data or "durations" not in data:
        raise RuntimeError(f"Unexpected Matrix API response: {data}")

    distance_df = pd.DataFrame(
        data["distances"],
        index=origins["point_name"].tolist(),
        columns=destinations["point_name"].tolist(),
    ).round(3)

    time_df = (
        pd.DataFrame(
            data["durations"],
            index=origins["point_name"].tolist(),
            columns=destinations["point_name"].tolist(),
        ) / 3600.0
    ).round(3)

    return distance_df, time_df


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
                print(
                    f"Rate limit hit for route "
                    f"({origin_lat}, {origin_lon}) -> ({destination_lat}, {destination_lon}). "
                    f"Retrying in {wait_time} seconds..."
                )
                time.sleep(wait_time)
                continue

            raise

    print(
        f"Warning: max retries exceeded for route "
        f"({origin_lat}, {origin_lon}) -> ({destination_lat}, {destination_lon})"
    )
    return None


# ============================================================
# SAVE TABLES
# ============================================================

def matrix_to_long_table(
    matrix_df: pd.DataFrame,
    relation_name: str,
    value_name: str,
    origin_label: str = "origin_name",
    destination_label: str = "destination_name",
) -> pd.DataFrame:
    long_df = (
        matrix_df.reset_index(names=origin_label)
        .melt(
            id_vars=[origin_label],
            var_name=destination_label,
            value_name=value_name,
        )
        .sort_values([origin_label, destination_label])
        .reset_index(drop=True)
    )

    long_df.insert(0, "relation", relation_name)
    return long_df


def save_relation_tables(
    conn_out: sqlite3.Connection,
    relation_name: str,
    distance_df: pd.DataFrame,
    time_df: pd.DataFrame,
) -> None:
    distance_table_name = f"{relation_name}_distance"
    time_table_name = f"{relation_name}_time"

    distance_long = matrix_to_long_table(
        matrix_df=distance_df,
        relation_name=relation_name,
        value_name="distance_km",
    )
    time_long = matrix_to_long_table(
        matrix_df=time_df,
        relation_name=relation_name,
        value_name="time_hours",
    )

    distance_long.to_sql(
        distance_table_name,
        conn_out,
        if_exists="replace",
        index=False,
    )
    time_long.to_sql(
        time_table_name,
        conn_out,
        if_exists="replace",
        index=False,
    )

    conn_out.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_{distance_table_name}_od
        ON {quote_identifier(distance_table_name)} (origin_name, destination_name)
        """
    )
    conn_out.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_{time_table_name}_od
        ON {quote_identifier(time_table_name)} (origin_name, destination_name)
        """
    )
    conn_out.commit()


# ============================================================
# ROUTE GEOMETRIES
# ============================================================

def compute_route_geometries(
    origins: pd.DataFrame,
    destinations: pd.DataFrame,
    profile: str = ORS_PROFILE,
    sleep_seconds: float = 1.2,
) -> list[dict]:
    route_records = []

    for _, origin_row in origins.iterrows():
        origin_name = origin_row["point_name"]
        origin_lat = float(origin_row["latitude"])
        origin_lon = float(origin_row["longitude"])

        for _, dest_row in destinations.iterrows():
            destination_name = dest_row["point_name"]
            destination_lat = float(dest_row["latitude"])
            destination_lon = float(dest_row["longitude"])

            print(f"Computing route geometry: {origin_name} -> {destination_name}")

            if is_same_location(
                origin_lat,
                origin_lon,
                destination_lat,
                destination_lon,
            ):
                print(f"Skipping same-location route: {origin_name} -> {destination_name}")
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
            except Exception as exc:
                print(f"Warning: route failed for {origin_name} -> {destination_name}: {exc}")
                geometry = None

            if geometry is not None:
                route_records.append(
                    {
                        "origin_name": origin_name,
                        "destination_name": destination_name,
                        "geometry": geometry,
                    }
                )
            else:
                print(f"Warning: no route returned for {origin_name} -> {destination_name}")

            time.sleep(sleep_seconds)

    return route_records


def build_route_gdf(route_records: list[dict]) -> gpd.GeoDataFrame:
    if not route_records:
        return gpd.GeoDataFrame(
            columns=["origin_name", "destination_name", "geometry"],
            geometry="geometry",
            crs="EPSG:4326",
        )

    return gpd.GeoDataFrame(route_records, geometry="geometry", crs="EPSG:4326")


# ============================================================
# PLOT
# ============================================================

def get_bounds(
    origins: pd.DataFrame,
    destinations: pd.DataFrame,
    route_gdf: gpd.GeoDataFrame,
    region_mode: str,
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

    if region_mode == "santiago":
        return (
            min_lon - 0.10,
            max_lon + 0.10,
            min_lat - 0.10,
            max_lat + 0.10,
        )

    return (
        min_lon - 0.50,
        max_lon + 0.50,
        min_lat - 0.50,
        max_lat + 0.50,
    )


def plot_relation_map(
    relation_name: str,
    origins: pd.DataFrame,
    destinations: pd.DataFrame,
    route_gdf: gpd.GeoDataFrame,
    communes_shp_path: Path,
    output_image: Path,
    region_mode: str,
) -> None:
    communes_gdf = gpd.read_file(communes_shp_path)

    if communes_gdf.crs is None:
        raise ValueError("The communes shapefile has no CRS defined.")

    communes_gdf = communes_gdf.to_crs(epsg=4326)

    min_lon, max_lon, min_lat, max_lat = get_bounds(
        origins=origins,
        destinations=destinations,
        route_gdf=route_gdf,
        region_mode=region_mode,
    )

    communes_plot = communes_gdf.cx[min_lon:max_lon, min_lat:max_lat]

    width = max_lon - min_lon
    height = max_lat - min_lat
    aspect_ratio = height / width if width > 0 else 1.0

    fig_width = 13
    fig_height = max(7, min(13, fig_width * aspect_ratio))

    fig, ax = plt.subplots(figsize=(fig_width, fig_height))

    if not communes_plot.empty:
        communes_plot.plot(
            ax=ax,
            facecolor="white",
            edgecolor="gray",
            linewidth=0.4,
            zorder=1,
        )

    color_map = {}

    if not route_gdf.empty:
        unique_origins = route_gdf["origin_name"].dropna().unique()
        cmap = plt.get_cmap("tab20")

        color_map = {
            origin_name: cmap(i % 20)
            for i, origin_name in enumerate(unique_origins)
        }

        for origin_name in unique_origins:
            subset = route_gdf[route_gdf["origin_name"] == origin_name]

            subset.plot(
                ax=ax,
                linewidth=2.5,
                color=color_map[origin_name],
                zorder=3,
                label=origin_name,
            )

    for _, row in origins.iterrows():
        ax.scatter(
            row["longitude"],
            row["latitude"],
            s=170,
            marker="o",
            color=color_map.get(row["point_name"], "black"),
            edgecolors="black",
            linewidths=0.8,
            zorder=4,
        )
        ax.text(
            row["longitude"],
            row["latitude"],
            row["point_name"],
            fontsize=8,
            fontweight="bold",
            zorder=5,
        )

    ax.scatter(
        destinations["longitude"],
        destinations["latitude"],
        s=120,
        marker="^",
        color="red",
        edgecolors="black",
        linewidths=0.8,
        zorder=4,
        label="Destinations",
    )

    for _, row in destinations.iterrows():
        ax.text(
            row["longitude"],
            row["latitude"],
            row["point_name"],
            fontsize=8,
            zorder=5,
        )

    ax.set_xlim(min_lon, max_lon)
    ax.set_ylim(min_lat, max_lat)
    ax.set_aspect("equal", adjustable="box")
    ax.set_title(f"{relation_name} heavy-truck routes")
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.legend(loc="best", fontsize=8)

    output_image.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_image, dpi=300, bbox_inches="tight")
    plt.close(fig)

    print(f"Map saved to: {output_image}")


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    validate_api_key()

    script_path = Path(__file__).resolve()
    project_root = script_path.parents[2]

    input_db_path = project_root / "Sets" / "model.db"
    output_db_path = project_root / "Sets" / "transport_matrices.db"
    communes_shp_path = (
        project_root
        / "extern data"
        / "DPA 2024"
        / "COMUNAS"
        / "COMUNAS_v1.shp"
    )
    output_graphs_dir = project_root / "Graphs" / "Routes"

    if not input_db_path.exists():
        raise FileNotFoundError(f"Input DB not found: {input_db_path}")

    if not communes_shp_path.exists():
        raise FileNotFoundError(f"Communes shapefile not found: {communes_shp_path}")

    print(f"Reading source DB: {input_db_path}")
    print(f"Writing output DB: {output_db_path}")

    with sqlite3.connect(input_db_path) as conn_in, sqlite3.connect(output_db_path) as conn_out:
        for relation_name, cfg in RELATIONS.items():
            print("=" * 80)
            print(f"Processing relation: {relation_name}")

            origin_cfg = cfg["origin"]
            destination_cfg = cfg["destination"]

            origins = load_points_from_table(
                conn=conn_in,
                table_name=origin_cfg["source"],
                name_col=origin_cfg["name_col"],
                type_filter=origin_cfg["type_filter"],
            )

            destinations = load_points_from_table(
                conn=conn_in,
                table_name=destination_cfg["source"],
                name_col=destination_cfg["name_col"],
                type_filter=destination_cfg["type_filter"],
            )

            print("Computing matrix API distance/time...")
            distance_df, time_df = call_matrix_api(
                origins=origins,
                destinations=destinations,
                profile=ORS_PROFILE,
            )

            save_relation_tables(
                conn_out=conn_out,
                relation_name=relation_name,
                distance_df=distance_df,
                time_df=time_df,
            )

            print(f"Saved tables: {relation_name}_distance and {relation_name}_time")

            print("Computing route geometries...")
            route_records = compute_route_geometries(
                origins=origins,
                destinations=destinations,
                profile=ORS_PROFILE,
                sleep_seconds=1.2,
            )

            route_gdf = build_route_gdf(route_records)

            output_image = output_graphs_dir / f"{relation_name}_routes.png"

            print("Plotting route map...")
            plot_relation_map(
                relation_name=relation_name,
                origins=origins,
                destinations=destinations,
                route_gdf=route_gdf,
                communes_shp_path=communes_shp_path,
                output_image=output_image,
                region_mode=cfg["plot_region"],
            )

    print("=" * 80)
    print("Done.")
    print(f"Output DB: {output_db_path}")
    print(f"Graphs dir: {output_graphs_dir}")


if __name__ == "__main__":
    main()