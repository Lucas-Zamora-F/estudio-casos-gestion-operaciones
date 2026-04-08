from __future__ import annotations

import random
from pathlib import Path

import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt

from shapely.geometry import Point
from shapely.ops import unary_union
from tqdm import tqdm


# ==========================================
# CONFIGURACIÓN
# ==========================================

BASE_DIR = Path("data") / "SOLVER DATA"
GRAPH_DIR = Path("graphs")

RUTA_SHP = Path("data") / "DPA 2024" / "COMUNAS" / "COMUNAS_v1.shp"
RUTA_CSV_CENSO = Path("data") / "CENSO" / "Base_manzana_entidad_CPV24.csv"

RUTA_CD = BASE_DIR / "CD.csv"
RUTA_DS = BASE_DIR / "DS.csv"
RUTA_MDCP = BASE_DIR / "MDCP.csv"
RUTA_Z = BASE_DIR / "Z.csv"
RUTA_SALIDA = BASE_DIR / "p(z).csv"

RADIO_CD_M = 15000
RADIO_DS_M = 5000
RADIO_MDCP_M = 10000

N_GRAFICOS = 20
SEED = 42
CHUNK_SIZE = 5000

# Margen del zoom en metros
ZOOM_PADDING_M = 8000


# ==========================================
# FUNCIONES AUXILIARES
# ==========================================

def crear_buffer(lon: float, lat: float, radio_m: float):
    punto = gpd.GeoSeries([Point(lon, lat)], crs="EPSG:4326").to_crs(epsg=32719)
    if radio_m <= 0:
        return punto.iloc[0]
    return punto.iloc[0].buffer(radio_m)


def cargar_instalaciones(ruta: Path, nombre_col: str) -> pd.DataFrame:
    df = pd.read_csv(ruta)
    cols_esperadas = [nombre_col, "latitud", "longitud"]
    faltantes = [c for c in cols_esperadas if c not in df.columns]
    if faltantes:
        raise ValueError(f"En {ruta} faltan columnas: {faltantes}")
    return df[cols_esperadas].copy()


def poblacion_cubierta(gdf_base: gpd.GeoDataFrame, geom_union) -> float:
    if geom_union is None or geom_union.is_empty:
        return 0.0

    temp = gdf_base.copy()
    temp["AREA_TOTAL"] = temp.geometry.area
    temp["INTER"] = temp.geometry.intersection(geom_union)
    temp["AREA_INTER"] = temp["INTER"].area

    temp["FRAC"] = 0.0
    mask = temp["AREA_TOTAL"] > 0
    temp.loc[mask, "FRAC"] = temp.loc[mask, "AREA_INTER"] / temp.loc[mask, "AREA_TOTAL"]

    temp["POB_CUB"] = temp["POBLACION_TOTAL"] * temp["FRAC"]
    return float(temp["POB_CUB"].sum())


def preparar_geografia() -> gpd.GeoDataFrame:
    gdf = gpd.read_file(RUTA_SHP)
    gdf["CUT_REG"] = gdf["CUT_REG"].astype(str).str.zfill(2)
    gdf["CUT_COM"] = gdf["CUT_COM"].astype(str).str.zfill(5)
    gdf = gdf[gdf["CUT_REG"] == "13"].copy()

    df = pd.read_csv(RUTA_CSV_CENSO, sep=";", low_memory=False)
    df["COD_REGION"] = pd.to_numeric(df["COD_REGION"], errors="coerce")
    df_rm = df[df["COD_REGION"] == 13].copy()
    df_rm["CUT"] = df_rm["CUT"].astype(str).str.zfill(5)

    df_comunas = (
        df_rm.groupby(["CUT", "COMUNA"], as_index=False)["n_per"]
        .sum()
        .rename(columns={"n_per": "POBLACION_TOTAL"})
    )

    gdf = gdf.merge(
        df_comunas[["CUT", "POBLACION_TOTAL"]],
        left_on="CUT_COM",
        right_on="CUT",
        how="left"
    )

    gdf["POBLACION_TOTAL"] = gdf["POBLACION_TOTAL"].fillna(0)
    gdf = gdf.to_crs(epsg=32719)

    return gdf


def construir_buffers(
    df_cd: pd.DataFrame,
    df_ds: pd.DataFrame,
    df_mdcp: pd.DataFrame
) -> tuple[dict[str, object], dict[str, tuple[float, float]]]:
    buffers = {}
    coords = {}

    for _, row in df_cd.iterrows():
        nombre = str(row["nombre_cd"])
        lat = float(row["latitud"])
        lon = float(row["longitud"])
        buffers[nombre] = crear_buffer(lon, lat, RADIO_CD_M)
        coords[nombre] = (lon, lat)

    for _, row in df_ds.iterrows():
        nombre = str(row["nombre_ds"])
        lat = float(row["latitud"])
        lon = float(row["longitud"])
        buffers[nombre] = crear_buffer(lon, lat, RADIO_DS_M)
        coords[nombre] = (lon, lat)

    for _, row in df_mdcp.iterrows():
        nombre = str(row["nombre_mdcp"])
        lat = float(row["latitud"])
        lon = float(row["longitud"])
        buffers[nombre] = crear_buffer(lon, lat, RADIO_MDCP_M)
        coords[nombre] = (lon, lat)

    return buffers, coords


def contar_filas_z(ruta_z: Path) -> int:
    with open(ruta_z, "r", encoding="utf-8-sig") as f:
        total = sum(1 for _ in f) - 1
    return max(total, 0)


def construir_mapa_colores(
    nombres_cd: list[str],
    nombres_ds: list[str],
    nombres_mdcp: list[str]
) -> dict[str, str]:
    """
    Asigna un color individual a cada instalación.
    """
    palette = [
        "tab:blue", "tab:orange", "tab:green", "tab:red", "tab:purple",
        "tab:brown", "tab:pink", "tab:gray", "tab:olive", "tab:cyan",
        "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
        "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf"
    ]

    nombres = nombres_cd + nombres_ds + nombres_mdcp
    if len(nombres) > len(palette):
        raise ValueError(
            f"Hay {len(nombres)} instalaciones y solo {len(palette)} colores definidos."
        )

    return {nombre: palette[i] for i, nombre in enumerate(nombres)}


def graficar_combinacion(
    gdf_base: gpd.GeoDataFrame,
    activos: list[str],
    coords: dict[str, tuple[float, float]],
    buffers: dict[str, object],
    colores_instalacion: dict[str, str],
    z_name: str,
    pob_total: float,
    salida_png: Path
) -> None:
    fig, ax = plt.subplots(figsize=(12, 12))

    # Si no hay activos, usar un zoom al centro aproximado de Santiago
    if not activos:
        xmin, xmax = 330000, 370000
        ymin, ymax = 6280000, 6320000
        gdf_plot = gdf_base.cx[xmin:xmax, ymin:ymax].copy()
    else:
        union_activos = unary_union([buffers[n] for n in activos])
        minx, miny, maxx, maxy = union_activos.bounds

        xmin = minx - ZOOM_PADDING_M
        xmax = maxx + ZOOM_PADDING_M
        ymin = miny - ZOOM_PADDING_M
        ymax = maxy + ZOOM_PADDING_M

        gdf_plot = gdf_base.cx[xmin:xmax, ymin:ymax].copy()
        if gdf_plot.empty:
            gdf_plot = gdf_base.copy()

    # Fondo por población
    gdf_plot.plot(
        column="POBLACION_TOTAL",
        cmap="OrRd",
        linewidth=0.4,
        edgecolor="black",
        legend=True,
        ax=ax
    )

    # Dibujar buffers activos, cada uno con color propio y etiqueta exacta en leyenda
    for nombre in activos:
        color = colores_instalacion[nombre]

        gpd.GeoSeries([buffers[nombre]], crs=gdf_base.crs).plot(
            ax=ax,
            facecolor=color,
            alpha=0.35,              # ↑ más visible
            edgecolor=color,
            linewidth=2.5            # ↑ borde fuerte
        )

    # Dibujar puntos activos con etiqueta exacta en leyenda
    for nombre in activos:
        lon, lat = coords[nombre]
        color = colores_instalacion[nombre]

        punto_gs = gpd.GeoSeries([Point(lon, lat)], crs="EPSG:4326").to_crs(gdf_base.crs)
        x = punto_gs.iloc[0].x
        y = punto_gs.iloc[0].y

        ax.scatter(
            x,
            y,
            s=70,
            c=color,
            edgecolors="black",
            linewidths=0.9,
            zorder=5,
            label=nombre
        )

        ax.annotate(
            nombre,
            (x, y),
            xytext=(6, 6),
            textcoords="offset points",
            fontsize=8,
            color="black",
            bbox=dict(
                boxstyle="round,pad=0.2",
                fc="white",
                ec=color,
                alpha=0.85
            ),
            zorder=6
        )

    ax.set_title(
        f"Combinación {z_name}\n"
        f"Instalaciones activas: {len(activos)} | "
        f"Población cubierta: {int(round(pob_total)):,}".replace(",", "."),
        fontsize=12
    )

    # Zoom al área de interés
    ax.set_xlim(xmin, xmax)
    ax.set_ylim(ymin, ymax)

    if activos:
        ax.legend(
            title="Instalaciones activas",
            loc="upper left",
            fontsize=8,
            title_fontsize=9,
            frameon=True
        )

    ax.set_axis_off()

    plt.tight_layout()
    plt.savefig(salida_png, dpi=300, bbox_inches="tight")
    plt.close(fig)


# ==========================================
# MAIN
# ==========================================

def main():
    random.seed(SEED)
    GRAPH_DIR.mkdir(parents=True, exist_ok=True)
    BASE_DIR.mkdir(parents=True, exist_ok=True)

    print("==========================================")
    print("CÁLCULO DE p(z)")
    print("==========================================")

    print("\n[1/6] Cargando instalaciones...")
    df_cd = cargar_instalaciones(RUTA_CD, "nombre_cd")
    df_ds = cargar_instalaciones(RUTA_DS, "nombre_ds")
    df_mdcp = cargar_instalaciones(RUTA_MDCP, "nombre_mdcp")

    nombres_cd = df_cd["nombre_cd"].astype(str).tolist()
    nombres_ds = df_ds["nombre_ds"].astype(str).tolist()
    nombres_mdcp = df_mdcp["nombre_mdcp"].astype(str).tolist()

    columnas_esperadas = nombres_cd + nombres_ds + nombres_mdcp
    colores_instalacion = construir_mapa_colores(nombres_cd, nombres_ds, nombres_mdcp)

    print(f"CD   : {len(df_cd)}")
    print(f"DS   : {len(df_ds)}")
    print(f"MDCP : {len(df_mdcp)}")
    print(f"Total instalaciones: {len(columnas_esperadas)}")

    print("\n[2/6] Cargando shapefile y censo...")
    gdf_base = preparar_geografia()

    print("[3/6] Construyendo buffers...")
    buffers, coords = construir_buffers(df_cd, df_ds, df_mdcp)

    print("[4/6] Preparando muestra aleatoria para gráficos...")
    total_filas_z = contar_filas_z(RUTA_Z)
    n_graficos_real = min(N_GRAFICOS, total_filas_z)

    indices_graficos = set()
    if n_graficos_real > 0:
        indices_graficos = set(random.sample(range(1, total_filas_z + 1), n_graficos_real))

    print(f"Filas Z: {total_filas_z:,}")
    print(f"Gráficos a generar: {len(indices_graficos)}")

    print("[5/6] Validando estructura de Z.csv...")
    z_header = pd.read_csv(RUTA_Z, nrows=0).columns.tolist()
    if not z_header or z_header[0] != "z_name":
        raise ValueError("La primera columna de Z.csv debe ser 'z_name'.")

    columnas_z_inst = z_header[1:]
    if columnas_z_inst != columnas_esperadas:
        raise ValueError(
            "Las columnas de instalaciones en Z.csv no coinciden exactamente con el orden:\n"
            "CD -> DS -> MDCP.\n"
            f"Esperado: {columnas_esperadas}\n"
            f"Encontrado: {columnas_z_inst}"
        )

    if RUTA_SALIDA.exists():
        RUTA_SALIDA.unlink()

    print("[6/6] Recorriendo Z.csv...")
    primera_escritura = True
    fila_global = 0
    graficos_generados = 0

    lector_chunks = pd.read_csv(RUTA_Z, chunksize=CHUNK_SIZE)

    with tqdm(total=total_filas_z, desc="Procesando combinaciones z", unit="z") as pbar:
        for chunk in lector_chunks:
            resultados_chunk = []

            for _, row in chunk.iterrows():
                fila_global += 1
                z_name = str(row["z_name"])

                activos = [col for col in columnas_z_inst if int(row[col]) == 1]

                if len(activos) == 0:
                    geom_union = None
                    pob = 0.0
                else:
                    geoms = [buffers[n] for n in activos]
                    geom_union = unary_union(geoms)
                    pob = poblacion_cubierta(gdf_base, geom_union)

                resultados_chunk.append({
                    "z": z_name,
                    "poblacion": round(pob, 4)
                })

                if fila_global in indices_graficos:
                    salida_png = GRAPH_DIR / f"{z_name}.png"
                    graficar_combinacion(
                        gdf_base=gdf_base,
                        activos=activos,
                        coords=coords,
                        buffers=buffers,
                        colores_instalacion=colores_instalacion,
                        z_name=z_name,
                        pob_total=pob,
                        salida_png=salida_png
                    )
                    graficos_generados += 1

                pbar.update(1)
                pbar.set_postfix({
                    "fila": fila_global,
                    "graficos": graficos_generados
                })

            df_resultados = pd.DataFrame(resultados_chunk)
            df_resultados.to_csv(
                RUTA_SALIDA,
                mode="w" if primera_escritura else "a",
                header=primera_escritura,
                index=False,
                encoding="utf-8-sig"
            )
            primera_escritura = False

    print("\n==========================================")
    print("FINALIZADO")
    print("==========================================")
    print(f"Archivo generado: {RUTA_SALIDA}")
    print(f"Gráficos guardados en: {GRAPH_DIR.resolve()}")
    print(f"Total de gráficos generados: {graficos_generados}")


if __name__ == "__main__":
    main()