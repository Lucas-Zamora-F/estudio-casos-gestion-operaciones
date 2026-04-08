from pathlib import Path

import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt


def load_csv(path: Path, name_col: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    expected_cols = [name_col, "latitude", "longitude"]
    missing_cols = [col for col in expected_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing columns in {path}: {missing_cols}")
    return df[expected_cols].copy()


def to_gdf(df: pd.DataFrame, lon_col: str = "longitude", lat_col: str = "latitude") -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        df.copy(),
        geometry=gpd.points_from_xy(df[lon_col], df[lat_col]),
        crs="EPSG:4326"
    )


def plot_points(ax, gdf: gpd.GeoDataFrame, name_col: str, color: str, label: str, dx: float = 5, dy: float = 5):
    gdf.plot(ax=ax, color=color, markersize=60, label=label)

    for _, row in gdf.iterrows():
        ax.annotate(
            row[name_col],
            (row.geometry.x, row.geometry.y),
            xytext=(dx, dy),
            textcoords="offset points",
            fontsize=8,
            bbox=dict(boxstyle="round,pad=0.15", fc="white", ec="none", alpha=0.75)
        )


def main():
    base_path = Path("data") / "SOLVER DATA"
    graph_path = Path("graphs")
    graph_path.mkdir(parents=True, exist_ok=True)

    chile_shp_path = Path("data") / "DPA 2024" / "COMUNAS" / "COMUNAS_v1.shp"

    # File paths
    a_path = base_path / "A.csv"
    cd_path = base_path / "CD.csv"
    ds_path = base_path / "DS.csv"
    mdcp_path = base_path / "MDCP.csv"
    p_path = base_path / "P.csv"
    s_path = base_path / "S.csv"
    wm_path = base_path / "WM.csv"

    # Load data
    df_a = load_csv(a_path, "airport")
    df_cd = load_csv(cd_path, "cd_name")
    df_ds = load_csv(ds_path, "ds_name")
    df_mdcp = load_csv(mdcp_path, "mdcp_name")
    df_p = load_csv(p_path, "port")
    df_s = load_csv(s_path, "origin")
    df_wm = load_csv(wm_path, "wholesale_market")

    # Split origins
    df_s_world = df_s[~df_s["origin"].str.startswith("Chile-", na=False)].copy()
    df_s_chile = df_s[df_s["origin"].str.startswith("Chile-", na=False)].copy()

    # Single Chile point for world map
    df_chile_world = pd.DataFrame(
        [{"origin": "Chile", "latitude": -35.675, "longitude": -71.543}]
    )

    # Convert to GeoDataFrames
    gdf_a = to_gdf(df_a)
    gdf_cd = to_gdf(df_cd)
    gdf_ds = to_gdf(df_ds)
    gdf_mdcp = to_gdf(df_mdcp)
    gdf_p = to_gdf(df_p)
    gdf_s_world = to_gdf(df_s_world)
    gdf_s_chile = to_gdf(df_s_chile)
    gdf_chile_world = to_gdf(df_chile_world)
    gdf_wm = to_gdf(df_wm)

    # Base world map
    world = gpd.read_file(
        "https://naturalearth.s3.amazonaws.com/110m_cultural/ne_110m_admin_0_countries.zip"
    )

    # Chile shapefile
    chile_map = gpd.read_file(chile_shp_path)

    # ---------------------------------
    # World map: international origins + Chile
    # ---------------------------------
    fig, ax = plt.subplots(figsize=(24, 14))
    world.plot(ax=ax, color="whitesmoke", edgecolor="gray", linewidth=0.5)

    plot_points(ax, gdf_s_world, "origin", "red", "International Origins", 6, 6)
    plot_points(ax, gdf_chile_world, "origin", "darkred", "Chile", 6, 6)

    ax.set_title("World Map: Supply Origins", fontsize=18)
    ax.legend(fontsize=10)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")

    plt.tight_layout()
    plt.savefig(graph_path / "world_map_origins.png", dpi=300, bbox_inches="tight")
    plt.close()

    # ---------------------------------
    # Chile map: Chile suppliers + ports + airport (ZOOMED)
    # ---------------------------------
    fig, ax = plt.subplots(figsize=(12, 16))

    chile_map.plot(ax=ax, color="whitesmoke", edgecolor="gray", linewidth=0.3)

    plot_points(ax, gdf_s_chile, "origin", "red", "Chile Suppliers", 6, 6)
    plot_points(ax, gdf_p, "port", "blue", "Ports", 6, -10)
    plot_points(ax, gdf_a, "airport", "green", "Airport", 6, 10)

    ax.set_xlim(-73.5, -69.5)
    ax.set_ylim(-36, -30)

    ax.set_title("Chile Map: Central Zone (Suppliers, Ports, Airport)", fontsize=16)
    ax.legend(fontsize=10)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")

    plt.tight_layout()
    plt.savefig(graph_path / "chile_supply_nodes.png", dpi=300, bbox_inches="tight")
    plt.close()

    # ---------------------------------
    # Santiago map: CD + DS + MDCP + WM
    # ---------------------------------
    fig, ax = plt.subplots(figsize=(16, 16))

    # Load commune shapefile and keep only Metropolitan Region
    santiago_map = gpd.read_file(chile_shp_path)
    santiago_map["CUT_REG"] = santiago_map["CUT_REG"].astype(str).str.zfill(2)
    santiago_map = santiago_map[santiago_map["CUT_REG"] == "13"].copy()
    santiago_map = santiago_map.to_crs("EPSG:4326")

    # Plot commune boundaries
    santiago_map.plot(ax=ax, color="whitesmoke", edgecolor="gray", linewidth=0.5)

    # Plot facilities
    plot_points(ax, gdf_cd, "cd_name", "red", "CD", 5, 5)
    plot_points(ax, gdf_ds, "ds_name", "blue", "DS", 5, 5)
    plot_points(ax, gdf_mdcp, "mdcp_name", "green", "MDCP", 5, 5)
    plot_points(ax, gdf_wm, "wholesale_market", "purple", "Wholesale Market", 5, 5)

    # Zoom to Santiago area
    ax.set_xlim(-70.95, -70.48)
    ax.set_ylim(-33.72, -33.28)

    ax.set_title("Santiago Map: CD, DS, MDCP, and Wholesale Markets", fontsize=18)
    ax.legend(fontsize=10)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.grid(True, linestyle="--", alpha=0.3)

    plt.tight_layout()
    plt.savefig(graph_path / "santiago_facilities_map.png", dpi=300, bbox_inches="tight")
    plt.close()

    print("[OK] Maps generated successfully.")
    print(f"[OK] World map saved at: {graph_path / 'world_map_origins.png'}")
    print(f"[OK] Chile map saved at: {graph_path / 'chile_supply_nodes.png'}")
    print(f"[OK] Santiago map saved at: {graph_path / 'santiago_facilities_map.png'}")


if __name__ == "__main__":
    main()