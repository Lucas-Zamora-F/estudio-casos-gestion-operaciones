import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import matplotlib.pyplot as plt


# ==========================================
# CONFIGURACIÓN
# ==========================================

ruta_shp = r"data\DPA 2024\COMUNAS\COMUNAS_v1.shp"
ruta_csv = r"data\CENSO\Base_manzana_entidad_CPV24.csv"

CDS = {
    "CD_1": (-70.65, -33.45),
    "CD_2": (-70.75, -33.40),
}

DSS = {
    "DS_1": (-70.60, -33.50),
}

RADIO_CD_M = 15000
RADIO_DS_M = 5000

archivo_salida = "mapa_cobertura_con_poblacion.png"


# ==========================================
# FUNCIONES
# ==========================================

def crear_buffer(lon, lat, radio_m):
    punto = gpd.GeoSeries([Point(lon, lat)], crs="EPSG:4326").to_crs(epsg=32719)
    return punto.iloc[0].buffer(radio_m)


def poblacion_cubierta(gdf, geom):
    temp = gdf.copy()
    temp["AREA_TOTAL"] = temp.geometry.area
    temp["INTER"] = temp.geometry.intersection(geom)
    temp["AREA_INTER"] = temp["INTER"].area

    temp["FRAC"] = 0.0
    mask = temp["AREA_TOTAL"] > 0
    temp.loc[mask, "FRAC"] = temp.loc[mask, "AREA_INTER"] / temp.loc[mask, "AREA_TOTAL"]

    temp["POB_CUB"] = temp["POBLACION_TOTAL"] * temp["FRAC"]
    return temp["POB_CUB"].sum()


# ==========================================
# 1. SHAPEFILE
# ==========================================

gdf = gpd.read_file(ruta_shp)
gdf["CUT_REG"] = gdf["CUT_REG"].astype(str).str.zfill(2)
gdf["CUT_COM"] = gdf["CUT_COM"].astype(str).str.zfill(5)
gdf = gdf[gdf["CUT_REG"] == "13"].copy()


# ==========================================
# 2. CENSO
# ==========================================

df = pd.read_csv(ruta_csv, sep=";", low_memory=False)
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


# ==========================================
# 3. PROYECCIÓN
# ==========================================

gdf = gdf.to_crs(epsg=32719)


# ==========================================
# 4. BUFFERS
# ==========================================

buffers = {}

for nombre, (lon, lat) in CDS.items():
    buffers[nombre] = crear_buffer(lon, lat, RADIO_CD_M)

for nombre, (lon, lat) in DSS.items():
    buffers[nombre] = crear_buffer(lon, lat, RADIO_DS_M)


# ==========================================
# 5. ÁREAS INDIVIDUALES
# ==========================================

areas_individuales = {nombre: geom.area for nombre, geom in buffers.items()}


# ==========================================
# 6. INTERSECCIONES DE A PARES
# ==========================================

intersecciones_pares = {}
nombres = list(buffers.keys())

for i in range(len(nombres)):
    for j in range(i + 1, len(nombres)):
        n1 = nombres[i]
        n2 = nombres[j]
        inter = buffers[n1].intersection(buffers[n2])
        intersecciones_pares[(n1, n2)] = inter.area


# ==========================================
# 7. INTERSECCIÓN TRIPLE
# ==========================================

interseccion_triple_area = 0.0
if len(nombres) == 3:
    interseccion_triple_area = (
        buffers[nombres[0]]
        .intersection(buffers[nombres[1]])
        .intersection(buffers[nombres[2]])
        .area
    )


# ==========================================
# 8. UNIÓN TOTAL
# ==========================================

union_total = None
for geom in buffers.values():
    union_total = geom if union_total is None else union_total.union(geom)

area_total = union_total.area


# ==========================================
# 9. POBLACIÓN
# ==========================================

pob_total = poblacion_cubierta(gdf, union_total)

pob_individual = {
    nombre: poblacion_cubierta(gdf, geom)
    for nombre, geom in buffers.items()
}


# ==========================================
# 10. PRINT EN TERMINAL
# ==========================================

print("\n====== RESUMEN ======")

print("\n--- ÁREAS INDIVIDUALES ---")
for nombre, area in areas_individuales.items():
    print(f"{nombre}:")
    print(f"  Área = {area:,.2f} m²")
    print(f"  Área = {area / 1_000_000:,.4f} km²")

print("\n--- ÁREAS COMPARTIDAS (PARES) ---")
for (n1, n2), area in intersecciones_pares.items():
    print(f"{n1} ∩ {n2}:")
    print(f"  Área compartida = {area:,.2f} m²")
    print(f"  Área compartida = {area / 1_000_000:,.4f} km²")

if len(nombres) == 3:
    print("\n--- INTERSECCIÓN TRIPLE ---")
    print(f"{nombres[0]} ∩ {nombres[1]} ∩ {nombres[2]}:")
    print(f"  Área compartida triple = {interseccion_triple_area:,.2f} m²")
    print(f"  Área compartida triple = {interseccion_triple_area / 1_000_000:,.4f} km²")

print("\n--- ÁREA TOTAL CUBIERTA SIN DOBLE CONTEO ---")
print(f"Área total = {area_total:,.2f} m²")
print(f"Área total = {area_total / 1_000_000:,.4f} km²")

print("\n--- POBLACIÓN CUBIERTA POR INSTALACIÓN ---")
for nombre, pob in pob_individual.items():
    print(f"{nombre}:")
    print(f"  Población cubierta = {int(round(pob)):,}".replace(",", "."))

print("\n--- POBLACIÓN TOTAL CUBIERTA SIN DOBLE CONTEO ---")
print(f"Población total cubierta = {int(round(pob_total)):,}".replace(",", "."))


# ==========================================
# 11. MAPA FINAL: POBLACIÓN + COBERTURA
# ==========================================

fig, ax = plt.subplots(figsize=(10, 10))

# Fondo por población
gdf.plot(
    column="POBLACION_TOTAL",
    cmap="OrRd",
    linewidth=0.4,
    edgecolor="black",
    legend=True,
    ax=ax
)

# Círculos encima
for nombre, geom in buffers.items():
    gpd.GeoSeries([geom], crs=gdf.crs).plot(ax=ax, alpha=0.25)

# Puntos
pts = [Point(lon, lat) for (lon, lat) in list(CDS.values()) + list(DSS.values())]
gpd.GeoSeries(pts, crs="EPSG:4326").to_crs(gdf.crs).plot(ax=ax, markersize=40)

ax.set_title(
    "Cobertura logística + población (RM)\n"
    f"Área total cubierta: {area_total:,.0f} m² | "
    f"Población total cubierta: {int(round(pob_total)):,}".replace(",", ".")
)

ax.set_axis_off()

plt.tight_layout()
plt.savefig(archivo_salida, dpi=300, bbox_inches="tight")
plt.show()

print(f"\nMapa guardado como: {archivo_salida}")