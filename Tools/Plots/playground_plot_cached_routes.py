import random
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt


# ======================================================================================
# CONFIG
# ======================================================================================

ROUTES_BASE = Path("Tools/Plots/routes")
OUTPUT_DIR = Path("Tools/Plots/playground")

N_PLOTS = 3
ROUTES_PER_PLOT = 8  # cuántas rutas por gráfico


# ======================================================================================
# LOAD ALL ROUTES
# ======================================================================================

def load_all_routes():
    files = list(ROUTES_BASE.rglob("*.geojson"))

    if not files:
        raise RuntimeError("No se encontraron rutas en routes/")

    print(f"Total rutas encontradas: {len(files)}")
    return files


# ======================================================================================
# LOAD GEOJSON
# ======================================================================================

def load_route(path):
    try:
        gdf = gpd.read_file(path)
        gdf["file"] = path.name
        return gdf
    except Exception as e:
        print(f"Error cargando {path}: {e}")
        return None


# ======================================================================================
# PLOT
# ======================================================================================

def plot_random_routes(all_files, plot_id):
    selected = random.sample(all_files, min(ROUTES_PER_PLOT, len(all_files)))

    gdfs = []
    for f in selected:
        gdf = load_route(f)
        if gdf is not None:
            gdfs.append(gdf)

    if not gdfs:
        print("No hay rutas válidas para plot")
        return

    full = gpd.GeoDataFrame(
        pd.concat(gdfs, ignore_index=True),
        geometry="geometry",
        crs="EPSG:4326",
    )

    fig, ax = plt.subplots(figsize=(10, 10))

    cmap = plt.get_cmap("tab10")

    for i, (_, row) in enumerate(full.iterrows()):
        gpd.GeoSeries([row.geometry]).plot(
            ax=ax,
            color=cmap(i % 10),
            linewidth=2.5,
        )

    ax.set_title(f"Random Routes Sample #{plot_id}")
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.grid(True)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / f"routes_sample_{plot_id}.png"

    plt.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close()

    print(f"Guardado: {out_path}")


# ======================================================================================
# MAIN
# ======================================================================================

def main():
    all_files = load_all_routes()

    for i in range(1, N_PLOTS + 1):
        plot_random_routes(all_files, i)

    print("\nDONE")


if __name__ == "__main__":
    import pandas as pd  # lazy import
    main()