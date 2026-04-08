from pathlib import Path
import time
import requests
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import shape


ORS_API_KEY = "eyJvcmciOiI1YjNjZTM1OTc4NTExMTAwMDFjZjYyNDgiLCJpZCI6Ijc0OGZiYTE4ZTQ2MDQ2YjM4YmEwYzFiYzQwOWVjYzJmIiwiaCI6Im11cm11cjY0In0="
ORS_BASE_URL = "https://api.openrouteservice.org"


def check_api_key() -> None:
    if not ORS_API_KEY:
        raise ValueError("API key is missing.")


def load_points(csv_path: Path, name_col: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)

    required_cols = {name_col, "latitude", "longitude"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(
            f"Missing columns in {csv_path}: {sorted(missing)}. "
            f"Found columns: {list(df.columns)}"
        )

    df = df[[name_col, "latitude", "longitude"]].copy()
    df["latitude"] = pd.to_numeric(df["latitude"], errors="raise")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="raise")

    return df


def call_matrix_api(
    ports: pd.DataFrame,
    distribution_centers: pd.DataFrame,
    profile: str = "driving-hgv",
) -> pd.DataFrame:
    url = f"{ORS_BASE_URL}/v2/matrix/{profile}"

    locations = []

    for _, row in ports.iterrows():
        locations.append([float(row["longitude"]), float(row["latitude"])])

    for _, row in distribution_centers.iterrows():
        locations.append([float(row["longitude"]), float(row["latitude"])])

    port_count = len(ports)
    cd_count = len(distribution_centers)

    sources = list(range(port_count))
    destinations = list(range(port_count, port_count + cd_count))

    payload = {
        "locations": locations,
        "sources": sources,
        "destinations": destinations,
        "metrics": ["distance"],
        "units": "km",
    }

    headers = {
        "Authorization": ORS_API_KEY,
        "Content-Type": "application/json; charset=utf-8",
        "Accept": "application/json",
    }

    response = requests.post(url, headers=headers, json=payload, timeout=120)

    if response.status_code != 200:
        raise RuntimeError(
            f"Matrix API error {response.status_code}: {response.text}"
        )

    data = response.json()

    if "distances" not in data:
        raise RuntimeError(f"Unexpected Matrix API response: {data}")

    distance_matrix = pd.DataFrame(
        data["distances"],
        index=ports["port"].tolist(),
        columns=distribution_centers["cd_name"].tolist(),
    )

    return distance_matrix.round(3)


def call_directions_api(
    origin_lon: float,
    origin_lat: float,
    destination_lon: float,
    destination_lat: float,
    profile: str = "driving-hgv",
):
    """
    Request route geometry directly as GeoJSON.
    Returns a shapely geometry or None.
    """
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

    headers = {
        "Authorization": ORS_API_KEY,
        "Content-Type": "application/json; charset=utf-8",
        "Accept": "application/geo+json, application/json",
    }

    response = requests.post(url, headers=headers, json=payload, timeout=120)

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


def compute_all_route_geometries(
    ports: pd.DataFrame,
    distribution_centers: pd.DataFrame,
    profile: str = "driving-hgv",
    sleep_seconds: float = 0.2,
) -> list:
    route_records = []

    for _, port_row in ports.iterrows():
        port_name = port_row["port"]
        origin_lat = port_row["latitude"]
        origin_lon = port_row["longitude"]

        for _, cd_row in distribution_centers.iterrows():
            cd_name = cd_row["cd_name"]
            destination_lat = cd_row["latitude"]
            destination_lon = cd_row["longitude"]

            print(f"Computing route geometry: {port_name} -> {cd_name}")

            try:
                geometry = call_directions_api(
                    origin_lon=origin_lon,
                    origin_lat=origin_lat,
                    destination_lon=destination_lon,
                    destination_lat=destination_lat,
                    profile=profile,
                )
            except Exception as e:
                print(f"Warning: route failed for {port_name} -> {cd_name}: {e}")
                geometry = None

            if geometry is not None:
                route_records.append(
                    {
                        "port": port_name,
                        "cd_name": cd_name,
                        "geometry": geometry,
                    }
                )
            else:
                print(f"Warning: no route returned for {port_name} -> {cd_name}")

            time.sleep(sleep_seconds)

    return route_records


def build_route_gdf(route_records: list) -> gpd.GeoDataFrame:
    if not route_records:
        return gpd.GeoDataFrame(
            columns=["port", "cd_name", "geometry"],
            geometry="geometry",
            crs="EPSG:4326",
        )

    return gpd.GeoDataFrame(route_records, geometry="geometry", crs="EPSG:4326")


def get_dynamic_map_bounds(
    ports: pd.DataFrame,
    distribution_centers: pd.DataFrame,
    route_gdf: gpd.GeoDataFrame,
    padding_lon: float = 0.20,
    padding_lat: float = 0.20,
):
    lon_values = ports["longitude"].tolist() + distribution_centers["longitude"].tolist()
    lat_values = ports["latitude"].tolist() + distribution_centers["latitude"].tolist()

    if not route_gdf.empty:
        minx, miny, maxx, maxy = route_gdf.total_bounds
        lon_values.extend([minx, maxx])
        lat_values.extend([miny, maxy])

    min_lon = min(lon_values) - padding_lon
    max_lon = max(lon_values) + padding_lon
    min_lat = min(lat_values) - padding_lat
    max_lat = max(lat_values) + padding_lat

    return min_lon, max_lon, min_lat, max_lat


def filter_mainland_communes(
    communes_gdf: gpd.GeoDataFrame,
    min_lon: float,
    max_lon: float,
    min_lat: float,
    max_lat: float,
) -> gpd.GeoDataFrame:
    """
    Keep only communes intersecting the dynamic plot window.
    This removes Antarctica / oceanic territories from the map.
    """
    return communes_gdf.cx[min_lon:max_lon, min_lat:max_lat]


def plot_routes_on_communes_map(
    communes_shp_path: Path,
    ports: pd.DataFrame,
    distribution_centers: pd.DataFrame,
    route_gdf: gpd.GeoDataFrame,
    output_image: Path,
):
    communes_gdf = gpd.read_file(communes_shp_path)

    if communes_gdf.crs is None:
        raise ValueError("The communes shapefile has no CRS defined.")

    communes_gdf = communes_gdf.to_crs(epsg=4326)

    min_lon, max_lon, min_lat, max_lat = get_dynamic_map_bounds(
        ports=ports,
        distribution_centers=distribution_centers,
        route_gdf=route_gdf,
        padding_lon=0.20,
        padding_lat=0.20,
    )

    communes_plot = filter_mainland_communes(
        communes_gdf=communes_gdf,
        min_lon=min_lon,
        max_lon=max_lon,
        min_lat=min_lat,
        max_lat=max_lat,
    )

    width = max_lon - min_lon
    height = max_lat - min_lat
    aspect_ratio = height / width if width > 0 else 1.0

    fig_width = 12
    fig_height = max(7, min(12, fig_width * aspect_ratio))

    fig, ax = plt.subplots(figsize=(fig_width, fig_height))

    if not communes_plot.empty:
        communes_plot.plot(
            ax=ax,
            facecolor="white",
            edgecolor="gray",
            linewidth=0.4,
            zorder=1,
        )

    if not route_gdf.empty:
        route_gdf.plot(
            ax=ax,
            linewidth=2.2,
            zorder=3,
        )

    ax.scatter(
        ports["longitude"],
        ports["latitude"],
        s=180,
        marker="^",
        zorder=4,
        label="Ports",
    )

    ax.scatter(
        distribution_centers["longitude"],
        distribution_centers["latitude"],
        s=100,
        marker="o",
        zorder=4,
        label="Distribution Centers",
    )

    for _, row in ports.iterrows():
        ax.text(
            row["longitude"],
            row["latitude"],
            row["port"],
            fontsize=9,
            zorder=5,
        )

    for _, row in distribution_centers.iterrows():
        ax.text(
            row["longitude"],
            row["latitude"],
            row["cd_name"],
            fontsize=8,
            zorder=5,
        )

    ax.set_xlim(min_lon, max_lon)
    ax.set_ylim(min_lat, max_lat)
    ax.set_aspect("equal", adjustable="box")
    ax.set_title("openrouteservice truck routes from ports to distribution centers")
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.legend(loc="best")

    output_image.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_image, dpi=300, bbox_inches="tight")
    plt.close(fig)

    print(f"Route map saved to: {output_image}")


def main():
    check_api_key()

    project_root = Path(__file__).resolve().parents[1]

    port_csv = project_root / "data" / "SOLVER DATA" / "P.csv"
    cd_csv = project_root / "data" / "SOLVER DATA" / "CD.csv"
    communes_shp = project_root / "data" / "DPA 2024" / "COMUNAS" / "COMUNAS_v1.shp"

    output_csv = project_root / "data" / "SOLVER DATA" / "D_CD_P.csv"
    output_image = project_root / "graphs" / "distances" / "D_CD_P_ors_routes.png"

    ports = load_points(port_csv, "port")
    distribution_centers = load_points(cd_csv, "cd_name")

    print("Computing distance matrix with openrouteservice Matrix API...")
    distance_matrix = call_matrix_api(
        ports=ports,
        distribution_centers=distribution_centers,
        profile="driving-hgv",
    )

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    distance_matrix.to_csv(output_csv, encoding="utf-8-sig", index_label="port")
    print(f"Distance matrix saved to: {output_csv}")

    print("Computing route geometries with openrouteservice Directions API...")
    route_records = compute_all_route_geometries(
        ports=ports,
        distribution_centers=distribution_centers,
        profile="driving-hgv",
        sleep_seconds=0.2,
    )

    route_gdf = build_route_gdf(route_records)

    print("Plotting routes on communes shapefile...")
    plot_routes_on_communes_map(
        communes_shp_path=communes_shp,
        ports=ports,
        distribution_centers=distribution_centers,
        route_gdf=route_gdf,
        output_image=output_image,
    )


if __name__ == "__main__":
    main()