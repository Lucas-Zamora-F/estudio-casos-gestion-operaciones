from pathlib import Path
import pandas as pd
import osmnx as ox
import matplotlib.pyplot as plt


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


def main():
    project_root = Path(__file__).resolve().parents[1]

    airport_csv = project_root / "data" / "SOLVER DATA" / "A.csv"
    cd_csv = project_root / "data" / "SOLVER DATA" / "CD.csv"
    output_image = project_root / "graphs" / "distances" / "RM_points_map.png"

    airports = load_points(airport_csv, "airport")
    distribution_centers = load_points(cd_csv, "cd_name")

    print("Downloading Metropolitan Region boundary...")
    region_gdf = ox.geocode_to_gdf("Región Metropolitana de Santiago, Chile")

    print("Downloading road network for the Metropolitan Region...")
    G = ox.graph_from_polygon(
        region_gdf.geometry.iloc[0],
        network_type="drive",
        simplify=True
    )

    output_image.parent.mkdir(parents=True, exist_ok=True)

    fig, ax = ox.plot_graph(
        G,
        node_size=0,
        edge_linewidth=0.35,
        edge_color="black",
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

    ax.set_title("Metropolitan Region road network with airports and distribution centers", fontsize=14)
    ax.legend()

    fig.savefig(output_image, dpi=300, bbox_inches="tight")
    plt.close(fig)

    print(f"Map saved to: {output_image}")


if __name__ == "__main__":
    main()