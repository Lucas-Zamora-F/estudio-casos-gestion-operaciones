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


def is_same_location(
    lat1: float,
    lon1: float,
    lat2: float,
    lon2: float,
    tol: float = 1e-5,
) -> bool:
    return abs(lat1 - lat2) < tol and abs(lon1 - lon2) < tol


def call_matrix_api(
    distribution_centers: pd.DataFrame,
    wholesale_markets: pd.DataFrame,
    profile: str = "driving-hgv",
) -> pd.DataFrame:
    url = f"{ORS_BASE_URL}/v2/matrix/{profile}"

    locations = []

    for _, row in distribution_centers.iterrows():
        locations.append([float(row["longitude"]), float(row["latitude"])])

    for _, row in wholesale_markets.iterrows():
        locations.append([float(row["longitude"]), float(row["latitude"])])

    cd_count = len(distribution_centers)
    wm_count = len(wholesale_markets)

    sources = list(range(cd_count))
    destinations = list(range(cd_count, cd_count + wm_count))

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
        index=distribution_centers["cd_name"].tolist(),
        columns=wholesale_markets["wholesale_market"].tolist(),
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


def call_directions_api_with_retry(
    origin_lon: float,
    origin_lat: float,
    destination_lon: float,
    destination_lat: float,
    profile: str = "driving-hgv",
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
        except Exception as e:
            error_text = str(e)

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


def compute_all_route_geometries(
    distribution_centers: pd.DataFrame,
    wholesale_markets: pd.DataFrame,
    profile: str = "driving-hgv",
    sleep_seconds: float = 1.2,
) -> list:
    route_records = []

    for _, cd_row in distribution_centers.iterrows():
        cd_name = cd_row["cd_name"]
        origin_lat = float(cd_row["latitude"])
        origin_lon = float(cd_row["longitude"])

        for _, wm_row in wholesale_markets.iterrows():
            wm_name = wm_row["wholesale_market"]
            destination_lat = float(wm_row["latitude"])
            destination_lon = float(wm_row["longitude"])

            print(f"Computing route geometry: {cd_name} -> {wm_name}")

            if is_same_location(
                origin_lat,
                origin_lon,
                destination_lat,
                destination_lon,
            ):
                print(f"Skipping same-location route: {cd_name} -> {wm_name}")
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
            except Exception as e:
                print(f"Warning: route failed for {cd_name} -> {wm_name}: {e}")
                geometry = None

            if geometry is not None:
                route_records.append(
                    {
                        "cd_name": cd_name,
                        "wholesale_market": wm_name,
                        "geometry": geometry,
                    }
                )
            else:
                print(f"Warning: no route returned for {cd_name} -> {wm_name}")

            time.sleep(sleep_seconds)

    return route_records


def build_route_gdf(route_records: list) -> gpd.GeoDataFrame:
    if not route_records:
        return gpd.GeoDataFrame(
            columns=["cd_name", "wholesale_market", "geometry"],
            geometry="geometry",
            crs="EPSG:4326",
        )

    return gpd.GeoDataFrame(route_records, geometry="geometry", crs="EPSG:4326")


def get_dynamic_map_bounds(
    distribution_centers: pd.DataFrame,
    wholesale_markets: pd.DataFrame,
    route_gdf: gpd.GeoDataFrame,
    padding_lon: float = 0.20,
    padding_lat: float = 0.20,
):
    lon_values = (
        distribution_centers["longitude"].tolist()
        + wholesale_markets["longitude"].tolist()
    )
    lat_values = (
        distribution_centers["latitude"].tolist()
        + wholesale_markets["latitude"].tolist()
    )

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
    return communes_gdf.cx[min_lon:max_lon, min_lat:max_lat]


def plot_routes_on_communes_map(
    communes_shp_path: Path,
    distribution_centers: pd.DataFrame,
    wholesale_markets: pd.DataFrame,
    route_gdf: gpd.GeoDataFrame,
    output_image: Path,
):
    communes_gdf = gpd.read_file(communes_shp_path)

    if communes_gdf.crs is None:
        raise ValueError("The communes shapefile has no CRS defined.")

    communes_gdf = communes_gdf.to_crs(epsg=4326)

    min_lon, max_lon, min_lat, max_lat = get_dynamic_map_bounds(
        distribution_centers=distribution_centers,
        wholesale_markets=wholesale_markets,
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

    cd_color_map = {}

    if not route_gdf.empty:
        unique_cds = route_gdf["cd_name"].dropna().unique()
        cmap = plt.get_cmap("tab10")
        cd_color_map = {
            cd_name: cmap(i % 10) for i, cd_name in enumerate(unique_cds)
        }

        for cd_name in unique_cds:
            routes_cd = route_gdf[route_gdf["cd_name"] == cd_name]

            routes_cd.plot(
                ax=ax,
                linewidth=2.4,
                color=cd_color_map[cd_name],
                zorder=3,
                label=cd_name,
            )

    for _, row in distribution_centers.iterrows():
        color = cd_color_map.get(row["cd_name"], "black")
        ax.scatter(
            row["longitude"],
            row["latitude"],
            s=160,
            marker="o",
            color=color,
            edgecolors="black",
            linewidths=0.8,
            zorder=4,
        )

    ax.scatter(
        wholesale_markets["longitude"],
        wholesale_markets["latitude"],
        s=130,
        marker="D",
        color="red",
        edgecolors="black",
        linewidths=0.8,
        zorder=4,
        label="Wholesale Markets",
    )

    for _, row in distribution_centers.iterrows():
        ax.text(
            row["longitude"],
            row["latitude"],
            row["cd_name"],
            fontsize=9,
            fontweight="bold",
            zorder=5,
        )

    for _, row in wholesale_markets.iterrows():
        ax.text(
            row["longitude"],
            row["latitude"],
            row["wholesale_market"],
            fontsize=8,
            zorder=5,
        )

    ax.set_xlim(min_lon, max_lon)
    ax.set_ylim(min_lat, max_lat)
    ax.set_aspect("equal", adjustable="box")
    ax.set_title("openrouteservice truck routes from distribution centers to wholesale markets")
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

    cd_csv = project_root / "data" / "SOLVER DATA" / "CD.csv"
    wm_csv = project_root / "data" / "SOLVER DATA" / "WM.csv"
    communes_shp = project_root / "data" / "DPA 2024" / "COMUNAS" / "COMUNAS_v1.shp"

    output_csv = project_root / "data" / "SOLVER DATA" / "D_DC_WM.csv"
    output_image = project_root / "graphs" / "distances" / "D_DC_WM_ors_routes.png"

    distribution_centers = load_points(cd_csv, "cd_name")
    wholesale_markets = load_points(wm_csv, "wholesale_market")

    print("Computing distance matrix with openrouteservice Matrix API...")
    distance_matrix = call_matrix_api(
        distribution_centers=distribution_centers,
        wholesale_markets=wholesale_markets,
        profile="driving-hgv",
    )

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    distance_matrix.to_csv(output_csv, encoding="utf-8-sig", index_label="cd_name")
    print(f"Distance matrix saved to: {output_csv}")

    print("Computing route geometries with openrouteservice Directions API...")
    route_records = compute_all_route_geometries(
        distribution_centers=distribution_centers,
        wholesale_markets=wholesale_markets,
        profile="driving-hgv",
        sleep_seconds=1.2,
    )

    route_gdf = build_route_gdf(route_records)

    print("Plotting routes on communes shapefile...")
    plot_routes_on_communes_map(
        communes_shp_path=communes_shp,
        distribution_centers=distribution_centers,
        wholesale_markets=wholesale_markets,
        route_gdf=route_gdf,
        output_image=output_image,
    )


if __name__ == "__main__":
    main()