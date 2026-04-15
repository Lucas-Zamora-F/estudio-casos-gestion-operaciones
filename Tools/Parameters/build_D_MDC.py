import sqlite3
import itertools
from math import inf
from pathlib import Path

import pandas as pd


# ============================================================
# CONFIG
# ============================================================
MODEL_DB_PATH = Path("Sets/model.db")
TRANSPORT_DB_PATH = Path("Sets/transport_matrices.db")
PARAMETERS_DB_PATH = Path("Sets/parameters.db")

Z_TABLE = "Z"
F_TABLE = "F"
DC_MDCP_TABLE = "DC_MDCP_distance"
MDCP_MDCP_TABLE = "MDCP_distance"
OUTPUT_TABLE = "D_MDC"


# ============================================================
# UTILS
# ============================================================
def print_banner(title: str) -> None:
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


def normalize_text(x) -> str:
    return str(x).strip()


def load_table_as_df(db_path: Path, table_name: str) -> pd.DataFrame:
    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    finally:
        conn.close()

    df.columns = [c.strip() for c in df.columns]
    return df


def get_sqlite_table_columns(db_path: Path, table_name: str) -> list[str]:
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(f"PRAGMA table_info({table_name})")
        rows = cur.fetchall()
        return [r[1] for r in rows]
    finally:
        conn.close()


# ============================================================
# LOAD FACILITIES
# ============================================================
def load_facilities(model_db_path: Path, f_table: str) -> tuple[list[str], list[str], list[str]]:
    f_df = load_table_as_df(model_db_path, f_table)

    required_cols = {"facility_name", "type"}
    missing = required_cols - set(f_df.columns)
    if missing:
        raise ValueError(
            f"La tabla {f_table} debe tener las columnas {required_cols}. "
            f"Faltan: {missing}"
        )

    f_df["facility_name"] = f_df["facility_name"].map(normalize_text)
    f_df["type"] = f_df["type"].map(normalize_text)

    dc_names = sorted(f_df.loc[f_df["type"] == "DC", "facility_name"].tolist())
    mdcp_names = sorted(f_df.loc[f_df["type"] == "MDCP", "facility_name"].tolist())
    all_facilities = sorted(f_df["facility_name"].tolist())

    if not dc_names:
        raise ValueError("No se encontraron instalaciones tipo 'DC' en la tabla F.")
    if not mdcp_names:
        raise ValueError("No se encontraron instalaciones tipo 'MDCP' en la tabla F.")

    return dc_names, mdcp_names, all_facilities


# ============================================================
# LOAD DISTANCES
# ============================================================
def load_long_distance_dict(
    transport_db_path: Path,
    table_name: str
) -> dict[tuple[str, str], float]:
    df = load_table_as_df(transport_db_path, table_name)

    required_cols = {"origin_name", "destination_name", "distance_km"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(
            f"La tabla {table_name} debe tener las columnas {required_cols}. "
            f"Faltan: {missing}"
        )

    dist = {}
    for _, row in df.iterrows():
        a = normalize_text(row["origin_name"])
        b = normalize_text(row["destination_name"])
        d = float(row["distance_km"])
        dist[(a, b)] = d

    return dist


def get_distance(
    dist_dict: dict[tuple[str, str], float],
    origin: str,
    destination: str,
    allow_reverse: bool = True
) -> float:
    if origin == destination:
        return 0.0

    key = (origin, destination)
    if key in dist_dict:
        return dist_dict[key]

    if allow_reverse:
        reverse_key = (destination, origin)
        if reverse_key in dist_dict:
            return dist_dict[reverse_key]

    raise KeyError(f"No existe distancia entre '{origin}' y '{destination}'.")


# ============================================================
# Z PROCESSING
# ============================================================
def get_active_facilities_from_z(
    row: pd.Series,
    candidate_facilities: list[str]
) -> list[str]:
    active = []
    for fac in candidate_facilities:
        if fac not in row.index:
            continue

        value = row[fac]

        try:
            active_flag = int(value)
        except Exception:
            continue

        if active_flag == 1:
            active.append(fac)

    return active


# ============================================================
# ROUTING
# ============================================================
def compute_route_distance(
    start_cd: str,
    mdcp_sequence: tuple[str, ...],
    dc_mdcp_dist: dict[tuple[str, str], float],
    mdcp_mdcp_dist: dict[tuple[str, str], float]
) -> float:
    if len(mdcp_sequence) == 0:
        return 0.0

    total_distance = 0.0

    # DC -> first MDCP
    total_distance += get_distance(dc_mdcp_dist, start_cd, mdcp_sequence[0], allow_reverse=True)

    # MDCP -> MDCP
    for i in range(len(mdcp_sequence) - 1):
        total_distance += get_distance(
            mdcp_mdcp_dist,
            mdcp_sequence[i],
            mdcp_sequence[i + 1],
            allow_reverse=True
        )

    # last MDCP -> same DC
    total_distance += get_distance(dc_mdcp_dist, start_cd, mdcp_sequence[-1], allow_reverse=True)

    return total_distance


def build_route_string(start_cd: str, mdcp_sequence: tuple[str, ...]) -> str:
    route = [start_cd] + list(mdcp_sequence) + [start_cd]
    return " -> ".join(route)


def find_best_route_for_z(
    active_cds: list[str],
    active_mdcps: list[str],
    dc_mdcp_dist: dict[tuple[str, str], float],
    mdcp_mdcp_dist: dict[tuple[str, str], float]
) -> tuple[str | None, float | None, tuple[str, ...] | None]:
    if not active_cds or not active_mdcps:
        return None, None, None

    best_cd = None
    best_distance = inf
    best_sequence = None

    for cd in active_cds:
        for perm in itertools.permutations(active_mdcps):
            route_distance = compute_route_distance(
                start_cd=cd,
                mdcp_sequence=perm,
                dc_mdcp_dist=dc_mdcp_dist,
                mdcp_mdcp_dist=mdcp_mdcp_dist
            )

            if route_distance < best_distance:
                best_distance = route_distance
                best_cd = cd
                best_sequence = perm

    return best_cd, best_distance, best_sequence


# ============================================================
# OUTPUT
# ============================================================
def save_results_to_parameters_db(
    results_df: pd.DataFrame,
    parameters_db_path: Path,
    output_table: str
) -> None:
    conn = sqlite3.connect(parameters_db_path)
    try:
        cur = conn.cursor()

        cur.execute(f"DROP TABLE IF EXISTS {output_table}")

        cur.execute(f"""
            CREATE TABLE {output_table} (
                z_name TEXT PRIMARY KEY,
                start_cd TEXT,
                min_distance_km REAL,
                route TEXT,
                n_active_cds INTEGER,
                n_active_mdcps INTEGER
            )
        """)

        results_df.to_sql(output_table, conn, if_exists="append", index=False)
        conn.commit()
    finally:
        conn.close()


# ============================================================
# MAIN
# ============================================================
def main():
    print_banner("BUILDING D_MDC FROM CURRENT SQLITE STRUCTURE")

    # --------------------------------------------------------
    # 1) Load facilities
    # --------------------------------------------------------
    dc_names, mdcp_names, all_facilities = load_facilities(MODEL_DB_PATH, F_TABLE)

    print(f"DC encontrados   : {len(dc_names)}")
    print(f"MDCP encontrados : {len(mdcp_names)}")
    print(f"Total facilities : {len(all_facilities)}")

    # --------------------------------------------------------
    # 2) Load Z
    # --------------------------------------------------------
    z_df = load_table_as_df(MODEL_DB_PATH, Z_TABLE)

    if "z_name" not in z_df.columns:
        raise ValueError("La tabla Z debe tener una columna llamada 'z_name'.")

    z_columns = set(z_df.columns)

    # Nos quedamos solo con facilities que existan como columnas en Z
    dc_cols_in_z = [c for c in dc_names if c in z_columns]
    mdcp_cols_in_z = [c for c in mdcp_names if c in z_columns]

    if not dc_cols_in_z:
        raise ValueError(
            "No encontré columnas de DC en Z que coincidan con F.facility_name."
        )

    if not mdcp_cols_in_z:
        raise ValueError(
            "No encontré columnas de MDCP en Z que coincidan con F.facility_name."
        )

    print(f"DC columns en Z   : {len(dc_cols_in_z)}")
    print(f"MDCP columns en Z : {len(mdcp_cols_in_z)}")

    # --------------------------------------------------------
    # 3) Load distances
    # --------------------------------------------------------
    dc_mdcp_dist = load_long_distance_dict(TRANSPORT_DB_PATH, DC_MDCP_TABLE)
    mdcp_mdcp_dist = load_long_distance_dict(TRANSPORT_DB_PATH, MDCP_MDCP_TABLE)

    print(f"Arcos {DC_MDCP_TABLE} : {len(dc_mdcp_dist)}")
    print(f"Arcos {MDCP_MDCP_TABLE}: {len(mdcp_mdcp_dist)}")

    # --------------------------------------------------------
    # 4) Process valid z
    # --------------------------------------------------------
    results = []
    total_rows = len(z_df)
    processed_rows = 0
    skipped_rows = 0

    for _, row in z_df.iterrows():
        z_name = normalize_text(row["z_name"])

        active_cds = get_active_facilities_from_z(row, dc_cols_in_z)
        active_mdcps = get_active_facilities_from_z(row, mdcp_cols_in_z)

        # Solo procesamos z con al menos 1 DC y 1 MDCP activos
        if len(active_cds) == 0 or len(active_mdcps) == 0:
            skipped_rows += 1
            continue

        try:
            best_cd, min_distance, best_sequence = find_best_route_for_z(
                active_cds=active_cds,
                active_mdcps=active_mdcps,
                dc_mdcp_dist=dc_mdcp_dist,
                mdcp_mdcp_dist=mdcp_mdcp_dist
            )
        except KeyError as e:
            print(f"[WARNING] z='{z_name}' omitido por distancia faltante: {e}")
            skipped_rows += 1
            continue

        if best_cd is None or best_sequence is None or min_distance is None:
            skipped_rows += 1
            continue

        route_str = build_route_string(best_cd, best_sequence)

        results.append({
            "z_name": z_name,
            "start_cd": best_cd,
            "min_distance_km": float(min_distance),
            "route": route_str,
            "n_active_cds": len(active_cds),
            "n_active_mdcps": len(active_mdcps),
        })

        processed_rows += 1

    # --------------------------------------------------------
    # 5) Save
    # --------------------------------------------------------
    results_df = pd.DataFrame(results)

    if results_df.empty:
        raise ValueError("No se generaron resultados. Revisa Z y las tablas de distancias.")

    save_results_to_parameters_db(results_df, PARAMETERS_DB_PATH, OUTPUT_TABLE)

    # --------------------------------------------------------
    # 6) Summary
    # --------------------------------------------------------
    print_banner("SUMMARY")
    print(f"Filas totales en Z     : {total_rows}")
    print(f"Filas procesadas       : {processed_rows}")
    print(f"Filas omitidas         : {skipped_rows}")
    print(f"Tabla creada en        : {PARAMETERS_DB_PATH}")
    print(f"Nombre de tabla        : {OUTPUT_TABLE}\n")

    print(results_df.head(10).to_string(index=False))


if __name__ == "__main__":
    main()