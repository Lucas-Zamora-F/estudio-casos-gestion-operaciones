from pathlib import Path
import json
import math
import requests
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt


API_KEY = "YOUR_GOOGLE_MAPS_API_KEY"


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


def decode_polyline(encoded: str):
    """
    Decode a Google encoded polyline into a list of (lat, lon) tuples.
    """
    points = []
    index = 0
    lat = 0
    lng = 0

    while index < len(encoded):
        shift = 0
        result = 0
        while True:
            b = ord(encoded[index]) - 63
            index += 1
            result |= (b & 0x1F) << shift
            shift += 5
            if b < 0x20:
                break
        delta_lat = ~(result >> 1) if (result & 1) else (result >> 1)
        lat += delta_lat

        shift = 0
        result = 0
        while True:
            b = ord(encoded[index]) - 63
            index += 1
            result |= (b & 0x1F) << shift
            shift += 5
            if b < 0x20:
                break
        delta_lng = ~(result >> 1) if (result & 1) else (result >> 1)
        lng += delta_lng

        points.append((lat / 1e5, lng / 1e5))

    return points


def build_waypoint(lat: float, lon: float) -> dict:
    return {
        "waypoint": {
            "location": {
                "latLng": {
                    "latitude": float(lat),
                    "longitude": float(lon),
                }
            }
        }
    }


def call_compute_route_matrix(airports: pd.DataFrame, distribution_centers: pd.DataFrame) -> list:
    url = "https://routes.googleapis.com/distanceMatrix/v2:computeRouteMatrix"

    payload = {
        "origins": [
            build_waypoint(row["latitude"], row["longitude"])
            for _, row in airports.iterrows()
        ],
        "destinations": [
            build_waypoint(row["latitude"], row["longitude"])
            for _, row in distribution_centers.iterrows()
        ],
        "travelMode": "DRIVE",
        "routingPreference": "TRAFFIC_UNAWARE",
        "units": "METRIC",
    }

    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": API_KEY,
        "X-Goog-FieldMask": (
            "originIndex,destinationIndex,status,"
            "distanceMeters,duration,condition"
        ),
    }

    response = requests.post(url, headers=headers, json=payload, timeout=120)
    response.raise_for_status()

    raw_text = response.text.strip()
    if not raw_text:
        raise RuntimeError("Empty response from computeRouteMatrix.")

    # computeRouteMatrix returns a streamed JSON response:
    # one JSON object per line
    matrix_elements = []
    for line in raw_text.splitlines():
        if line.strip():
            matrix_elements.append(json.loads(line))

    return matrix_elements


def build_distance_matrix(
    matrix_elements: list,
    airports: pd.DataFrame,
    distribution_centers: pd.DataFrame,
) -> pd.DataFrame:
    distance_matrix = pd.DataFrame(
        index=airports["airport"],
        columns=distribution_centers["cd_name"],
        dtype=float,
    )

    for element in matrix_elements:
        origin_index = element.get("originIndex")
        destination_index = element.get("destinationIndex")
        condition = element.get("condition")
        distance_meters = element.get("distanceMeters")

        airport_name = airports.iloc[origin_index]["airport"]
        cd_name = distribution_centers.iloc[destination_index]["cd_name"]

        if condition == "ROUTE_EXISTS" and distance_meters is not None:
            distance_matrix.loc[airport_name, cd_name] = round(distance_meters / 1000.0, 3)
        else:
            distance_matrix.loc[airport_name, cd_name] = None

    return distance_matrix


def call_compute_route(origin_lat: float, origin_lon: float, dest_lat: float, dest_lon: float) -> list:
    url = "https://routes.googleapis.com/directions/v2:computeRoutes"

    payload = {
        "origin": {
            "location": {
                "latLng": {
                    "latitude": float(origin_lat),
                    "longitude": float(origin_lon),
                }
            }
        },
        "destination": {
            "location": {
                "latLng": {
                    "latitude": float(dest_lat),
                    "longitude": float(dest_lon),
                }
            }
        },
        "travelMode": "DRIVE",
        "routingPreference": "TRAFFIC_UNAWARE",
        "polylineQuality": "OVERVIEW",
        "polylineEncoding": "ENCODED_POLYLINE",
        "computeAlternativeRoutes": False,
        "units": "METRIC",
    }

    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": API_KEY,
        "X-Goog-FieldMask": (
            "routes.distanceMeters,"
            "routes.duration,"
            "routes.polyline.encodedPolyline"
        ),
    }

    response = requests.post(url, headers=headers, json=payload, timeout=120)
    response.raise_for_status()

    data = response.json()
    routes = data.get("routes", [])
    if not routes:
        return []

    encoded_polyline = routes[0].get("polyline", {}).get("encodedPolyline")
    if not encoded_polyline:
        return []

    return decode_polyline(encoded_polyline)


def compute_all_route_geometries(
    airports: pd.DataFrame,
    distribution_centers: pd.DataFrame,
) -> list:
    route_records = []

    for _, airport_row in airports.iterrows():
        airport_name = airport_row["airport"]
        origin_lat = airport_row["latitude"]
        origin_lon = airport_row["longitude"]

        for _, cd_row in distribution_centers.iterrows():
            cd_name = cd_row["cd_name"]
            dest_lat = cd_row["latitude"]
            dest_lon = cd_row["longitude"]

            print(f"Computing route geometry: {airport_name} -> {cd_name}")

            route_points = call_compute_route(
                origin_lat=origin_lat,
                origin_lon=origin_lon,
                dest_lat=dest_lat,
                dest_lon=dest_lon,
            )

            if route_points:
                route_records.append(
                    {
                        "airport": airport_name,
                        "cd_name": cd_name,
                        "route_points": route_points,
                    }
                )
            else:
                print(f"Warning: no route geometry returned for {airport_name} -> {cd_name}")

    return route_records


def get_plot_bounds(airports: pd.DataFrame, distribution_centers: pd.DataFrame, route_records: list, padding: float = 0.08):
    lat_values = []
    lon_values = []

    for _, row in airports.iterrows():
        lat_values.append(row["latitude"])
        lon_values.append(row["longitude"])

    for _, row in distribution_centers.iterrows():
        lat_values.append(row["latitude"])
        lon_values.append(row["longitude"])

    for record in route_records:
        for lat, lon in record["route_points"]:
            lat_values.append(lat)
            lon_values.append(lon)

    min_lat = min(lat_values) - padding
    max_lat = max(lat_values) + padding
    min_lon = min(lon_values) - padding
    max_lon = max(lon_values) + padding

    return min_lon, max_lon, min_lat, max_lat


def plot_routes_on_communes_map(
    communes_shp_path: Path,
    airports: pd.DataFrame,
    distribution_centers: pd.DataFrame,
    route_records: list,
    output_image: Path,
):
    communes_gdf = gpd.read_file(communes_shp_path)

    if communes_gdf.crs is None:
        raise ValueError("The communes shapefile has no CRS defined.")

    communes_gdf = communes_gdf.to_crs(epsg=4326)

    min_lon, max_lon, min_lat, max_lat = get_plot_bounds(
        airports,
        distribution_centers,
        route_records,
        padding=0.05,
    )

    clipped_communes = communes_gdf.cx[min_lon:max_lon, min_lat:max_lat]

    fig, ax = plt.subplots(figsize=(12, 12))

    if len(clipped_communes) > 0:
        clipped_communes.plot(
            ax=ax,
            facecolor="white",
            edgecolor="gray",
            linewidth=0.4,
            zorder=1,
        )
    else:
        communes_gdf.plot(
            ax=ax,
            facecolor="white",
            edgecolor="gray",
            linewidth=0.4,
            zorder=1,
        )

    for record in route_records:
        route_points = record["route_points"]
        lats = [p[0] for p in route_points]
        lons = [p[1] for p in route_points]

        ax.plot(
            lons,
            lats,
            linewidth=2.0,
            zorder=3,
            label=f'{record["airport"]} -> {record["cd_name"]}',
        )

    ax.scatter(
        airports["longitude"],
        airports["latitude"],
        s=180,
        marker="^",
        zorder=4,
        label="Airports",
    )

    ax.scatter(
        distribution_centers["longitude"],
        distribution_centers["latitude"],
        s=100,
        marker="o",
        zorder=4,
        label="Distribution Centers",
    )

    for _, row in airports.iterrows():
        ax.text(
            row["longitude"],
            row["latitude"],
            row["airport"],
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
    ax.set_title("Google Routes API road routes from airports to distribution centers")
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")

    # Keep only one legend entry per point type
    handles, labels = ax.get_legend_handles_labels()
    seen = set()
    filtered_handles = []
    filtered_labels = []

    for handle, label in zip(handles, labels):
        if label not in seen:
            seen.add(label)
            filtered_handles.append(handle)
            filtered_labels.append(label)

    ax.legend(filtered_handles, filtered_labels, loc="best", fontsize=8)

    output_image.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_image, dpi=300, bbox_inches="tight")
    plt.close(fig)

    print(f"Route map saved to: {output_image}")


def main():
    project_root = Path(__file__).resolve().parents[1]

    airport_csv = project_root / "data" / "SOLVER DATA" / "A.csv"
    cd_csv = project_root / "data" / "SOLVER DATA" / "CD.csv"
    communes_shp = project_root / "data" / "DPA 2024" / "COMUNAS" / "COMUNAS_v1.shp"

    output_csv = project_root / "data" / "SOLVER DATA" / "D_CD_A.csv"
    output_image = project_root / "graphs" / "distances" / "D_CD_A_google_routes.png"

    airports = load_points(airport_csv, "airport")
    distribution_centers = load_points(cd_csv, "cd_name")

    print("Computing route matrix with Google Routes API...")
    matrix_elements = call_compute_route_matrix(airports, distribution_centers)

    print("Building distance matrix...")
    distance_matrix = build_distance_matrix(
        matrix_elements=matrix_elements,
        airports=airports,
        distribution_centers=distribution_centers,
    )

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    distance_matrix.to_csv(output_csv, encoding="utf-8-sig")
    print(f"Distance matrix saved to: {output_csv}")

    print("Computing detailed route geometries...")
    route_records = compute_all_route_geometries(
        airports=airports,
        distribution_centers=distribution_centers,
    )

    print("Plotting routes on communes map...")
    plot_routes_on_communes_map(
        communes_shp_path=communes_shp,
        airports=airports,
        distribution_centers=distribution_centers,
        route_records=route_records,
        output_image=output_image,
    )


if __name__ == "__main__":
    main()