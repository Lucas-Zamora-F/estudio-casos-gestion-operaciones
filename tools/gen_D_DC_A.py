from pathlib import Path
import pandas as pd
import osmnx as ox
import networkx as nx
import matplotlib.pyplot as plt
from shapely.geometry import box


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


def build_drive_graph(airports: pd.DataFrame, distribution_centers: pd.DataFrame, margin_deg: float = 0.08):
    all_points = pd.concat(
        [
            airports[["latitude", "longitude"]],
            distribution_centers[["latitude", "longitude"]],
        ],
        ignore_index=True
    )

    north = all_points["latitude"].max() + margin_deg
    south = all_points["latitude"].min() - margin_deg
    east = all_points["longitude"].max() + margin_deg
    west = all_points["longitude"].min() - margin_deg

    polygon = box(west, south, east, north)

    print("Downloading road network from OpenStreetMap...")
    G = ox.graph_from_polygon(
        polygon,
        network_type="drive",
        simplify=True
    )

    return G


def compute_routes_and_distance_matrix(G, airports: pd.DataFrame, distribution_centers: pd.DataFrame):
    distance_matrix = pd.DataFrame(
        index=airports["airport"],
        columns=distribution_centers["cd_name"],
        dtype=float
    )

    routes = []
    route_labels = []

    for _, airport_row in airports.iterrows():
        airport_name = airport_row["airport"]
        airport_lat = airport_row["latitude"]
        airport_lon = airport_row["longitude"]

        try:
            origin_node = ox.distance.nearest_nodes(G, X=airport_lon, Y=airport_lat)
        except Exception as e:
            raise RuntimeError(f"Could not map airport '{airport_name}' to the road network: {e}")

        for _, cd_row in distribution_centers.iterrows():
            cd_name = cd_row["cd_name"]
            cd_lat = cd_row["latitude"]
            cd_lon = cd_row["longitude"]

            try:
                destination_node = ox.distance.nearest_nodes(G, X=cd_lon, Y=cd_lat)
            except Exception as e:
                raise RuntimeError(f"Could not map distribution center '{cd_name}' to the road network: {e}")

            try:
                route = nx.shortest_path(G, origin_node, destination_node, weight="length")
                distance_m = nx.shortest_path_length(G, origin_node, destination_node, weight="length")
                distance_km = distance_m / 1000.0

                distance_matrix.loc[airport_name, cd_name] = round(distance_km, 3)
                routes.append(route)
                route_labels.append(f"{airport_name} -> {cd_name}")

                print(f"Route computed: {airport_name} -> {cd_name}: {distance_km:.3f} km")

            except nx.NetworkXNoPath:
                print(f"No path found between {airport_name} and {cd_name}")
                distance_matrix.loc[airport_name, cd_name] = None

    return distance_matrix, routes, route_labels


def plot_routes(
    G,
    airports: pd.DataFrame,
    distribution_centers: pd.DataFrame,
    routes,
    output_path: Path
):
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fig, ax = ox.plot_graph_routes(
        G,
        routes,
        route_linewidth=3,
        route_alpha=0.8,
        orig_dest_size=0,
        node_size=0,
        edge_linewidth=0.6,
        edge_alpha=0.35,
        bgcolor="white",
        show=False,
        close=False
    )

    ax.scatter(
        airports["longitude"],
        airports["latitude"],
        s=120,
        marker="^",
        label="Airports",
        zorder=5
    )

    ax.scatter(
        distribution_centers["longitude"],
        distribution_centers["latitude"],
        s=90,
        marker="o",
        label="Distribution Centers",
        zorder=5
    )

    for _, row in airports.iterrows():
        ax.text(
            row["longitude"],
            row["latitude"],
            row["airport"],
            fontsize=9,
            zorder=6
        )

    for _, row in distribution_centers.iterrows():
        ax.text(
            row["longitude"],
            row["latitude"],
            row["cd_name"],
            fontsize=8,
            zorder=6
        )

    ax.set_title("Shortest road routes from airports to distribution centers", fontsize=14)
    ax.legend()

    fig.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close(fig)

    print(f"Route map saved to: {output_path}")


def main():
    project_root = Path(__file__).resolve().parents[1]

    airport_csv = project_root / "data" / "SOLVER DATA" / "A.csv"
    cd_csv = project_root / "data" / "SOLVER DATA" / "CD.csv"

    output_csv = project_root / "data" / "SOLVER DATA" / "D_CD_A.csv"
    output_image = project_root / "graphs" / "distances" / "D_CD_A_routes.png"

    airports = load_points(airport_csv, "airport")
    distribution_centers = load_points(cd_csv, "cd_name")

    G = build_drive_graph(airports, distribution_centers, margin_deg=0.08)

    distance_matrix, routes, route_labels = compute_routes_and_distance_matrix(
        G,
        airports,
        distribution_centers
    )

    if len(routes) == 0:
        raise RuntimeError("No routes could be computed.")

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    distance_matrix.to_csv(output_csv, index=True, encoding="utf-8-sig")

    print(f"Distance matrix saved to: {output_csv}")

    plot_routes(G, airports, distribution_centers, routes, output_image)


if __name__ == "__main__":
    main()