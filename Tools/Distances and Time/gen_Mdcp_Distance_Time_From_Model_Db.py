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
# FILE NAME: gen_mdcp_distance_time_from_model_db.py
# ============================================================

ORS_API_KEY = "eyJvcmciOiI1YjNjZTM1OTc4NTExMTAwMDFjZjYyNDgiLCJpZCI6Ijc0OGZiYTE4ZTQ2MDQ2YjM4YmEwYzFiYzQwOWVjYzJmIiwiaCI6Im11cm11cjY0In0="
ORS_BASE_URL = "https://api.openrouteservice.org"
ORS_PROFILE = "driving-hgv"


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

def load_mdcp_points(conn: sqlite3.Connection) -> pd.DataFrame:
    sql = f"""
        SELECT
            {quote_identifier('facility_name')} AS point_name,
            type,
            latitude,
            longitude
        FROM {quote_identifier('F')}
        WHERE type = 'MDCP'
    """

    df = pd.read_sql(sql, conn)

    required = {"point_name", "type", "latitude", "longitude"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in F: {sorted(missing)}")

    df["latitude"] = pd.to_numeric(df["latitude"], errors="raise")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="raise")

    if df.empty:
        raise ValueError("No MDCP rows found in table F.")

    return df[["point_name", "type", "latitude", "longitude"]].reset_index(drop=True)


# ============================================================
# ORS MATRIX
# ============================================================

def call_matrix_api_mdcp(
    mdcps: pd.DataFrame,
    profile: str = ORS_PROFILE,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    url = f"{ORS_BASE_URL}/v2/matrix/{profile}"

    locations = []
    for _, row in mdcps.iterrows():
        locations.append([float(row["longitude"]), float(row["latitude"])])

    payload = {
        "locations": locations,
        "sources": list(range(len(mdcps))),
        "destinations": list(range(len(mdcps))),
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

    names = mdcps["point_name"].tolist()

    distance_df = pd.DataFrame(data["distances"], index=names, columns=names).round(3)
    time_df = (pd.DataFrame(data["durations"], index=names, columns=names) / 3600.0).round(3)

    for name in names:
        distance_df.loc[name, name] = 0.0
        time_df.loc[name, name] = 0.0

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
        raise RuntimeError(f"Directions API error {response.status_code}: {response.text}")

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


def save_mdcp_tables(
    conn_out: sqlite3.Connection,
    distance_df: pd.DataFrame,
    time_df: pd.DataFrame,
) -> None:
    distance_long = matrix_to_long_table(
        matrix_df=distance_df,
        relation_name="MDCP",
        value_name="distance_km",
    )
    time_long = matrix_to_long_table(
        matrix_df=time_df,
        relation_name="MDCP",
        value_name="time_hours",
    )

    distance_long.to_sql("MDCP_distance", conn_out, if_exists="replace", index=False)
    time_long.to_sql("MDCP_time", conn_out, if_exists="replace", index=False)

    conn_out.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_MDCP_distance_od
        ON MDCP_distance (origin_name, destination_name)
        """
    )
    conn_out.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_MDCP_time_od
        ON MDCP_time (origin_name, destination_name)
        """
    )
    conn_out.commit()


# ============================================================
# ROUTE GEOMETRIES
# ============================================================

def compute_mdcp_route_geometries(
    mdcps: pd.DataFrame,
    profile: str = ORS_PROFILE,
    sleep_seconds: float = 1.2,
) -> list[dict]:
    route_records = []

    for i in range(len(mdcps)):
        origin_row = mdcps.iloc[i]
        origin_name = origin_row["point_name"]
        origin_lat = float(origin_row["latitude"])
        origin_lon = float(origin_row["longitude"])

        for j in range(i + 1, len(mdcps)):
            dest_row = mdcps.iloc[j]
            destination_name = dest_row["point_name"]
            destination_lat = float(dest_row["latitude"])
            destination_lon = float(dest_row["longitude"])

            print(f"Computing route geometry: {origin_name} -> {destination_name}")

            if is_same_location(origin_lat, origin_lon, destination_lat, destination_lon):
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
    mdcps: pd.DataFrame,
    route_gdf: gpd.GeoDataFrame,
) -> tuple[float, float, float, float]:
    lon_values = mdcps["longitude"].tolist()
    lat_values = mdcps["latitude"].tolist()

    if not route_gdf.empty:
        minx, miny, maxx, maxy = route_gdf.total_bounds
        lon_values.extend([minx, maxx])
        lat_values.extend([miny, maxy])

    return (
        min(lon_values) - 0.10,
        max(lon_values) + 0.10,
        min(lat_values) - 0.10,
        max(lat_values) + 0.10,
    )


def plot_mdcp_map(
    mdcps: pd.DataFrame,
    route_gdf: gpd.GeoDataFrame,
    communes_shp_path: Path,
    output_image: Path,
) -> None:
    communes_gdf = gpd.read_file(communes_shp_path)

    if communes_gdf.crs is None:
        raise ValueError("The communes shapefile has no CRS defined.")

    communes_gdf = communes_gdf.to_crs(epsg=4326)

    min_lon, max_lon, min_lat, max_lat = get_bounds(mdcps=mdcps, route_gdf=route_gdf)
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

    # -----------------------------
    # Color each route differently
    # -----------------------------
    if not route_gdf.empty:
        cmap = plt.get_cmap("tab20")

        for i, (_, row) in enumerate(route_gdf.iterrows()):
            gpd.GeoSeries([row.geometry], crs="EPSG:4326").plot(
                ax=ax,
                linewidth=2.4,
                color=cmap(i % 20),
                zorder=3,
                label=f"{row['origin_name']}→{row['destination_name']}" if i < 15 else None,
            )

    # Plot MDCP points
    ax.scatter(
        mdcps["longitude"],
        mdcps["latitude"],
        s=150,
        marker="D",
        color="red",
        edgecolors="black",
        linewidths=0.8,
        zorder=4,
        label="MDCP",
    )

    for _, row in mdcps.iterrows():
        ax.text(
            row["longitude"],
            row["latitude"],
            row["point_name"],
            fontsize=8,
            fontweight="bold",
            zorder=5,
        )

    ax.set_xlim(min_lon, max_lon)
    ax.set_ylim(min_lat, max_lat)
    ax.set_aspect("equal", adjustable="box")
    ax.set_title("MDCP heavy-truck routes")
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.legend(loc="best", fontsize=7)

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
    output_image = project_root / "Graphs" / "Routes" / "MDCP_routes.png"

    if not input_db_path.exists():
        raise FileNotFoundError(f"Input DB not found: {input_db_path}")

    if not communes_shp_path.exists():
        raise FileNotFoundError(f"Communes shapefile not found: {communes_shp_path}")

    print(f"Reading source DB: {input_db_path}")
    print(f"Writing output DB: {output_db_path}")

    with sqlite3.connect(input_db_path) as conn_in, sqlite3.connect(output_db_path) as conn_out:
        mdcps = load_mdcp_points(conn_in)

        print("Computing MDCP matrix API distance/time...")
        distance_df, time_df = call_matrix_api_mdcp(mdcps=mdcps, profile=ORS_PROFILE)

        save_mdcp_tables(conn_out=conn_out, distance_df=distance_df, time_df=time_df)
        print("Saved tables: MDCP_distance and MDCP_time")

        print("Computing MDCP route geometries...")
        route_records = compute_mdcp_route_geometries(
            mdcps=mdcps,
            profile=ORS_PROFILE,
            sleep_seconds=1.2,
        )

        route_gdf = build_route_gdf(route_records)

        print("Plotting MDCP route map...")
        plot_mdcp_map(
            mdcps=mdcps,
            route_gdf=route_gdf,
            communes_shp_path=communes_shp_path,
            output_image=output_image,
        )

    print("=" * 80)
    print("Done.")
    print(f"Output DB: {output_db_path}")
    print(f"Map: {output_image}")


if __name__ == "__main__":
    main()
