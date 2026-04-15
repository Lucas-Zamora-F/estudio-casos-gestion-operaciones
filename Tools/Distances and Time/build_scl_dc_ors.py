import os
import time
import sqlite3
from pathlib import Path
from typing import List, Tuple, Optional

import pandas as pd
import requests


# ============================================================
# CONFIG
# ============================================================
MODEL_DB = Path("Sets/model.db")
TRANSPORT_DB = Path("Sets/transport_matrices.db")

ORS_API_KEY = os.getenv("ORS_API_KEY", "").strip()

PRIMARY_PROFILE = "driving-hgv"
FALLBACK_PROFILE = "driving-car"

MAX_RETRIES = 4
RETRY_SLEEP_SEC = 3
REQUEST_SLEEP_SEC = 0.4

ORS_BASE_URL = "https://api.openrouteservice.org"

OVERWRITE_EXISTING = True


# ============================================================
# LOGGING
# ============================================================
def log(msg: str) -> None:
    print(msg)


def section(title: str) -> None:
    print("\n" + "=" * 90)
    print(title)
    print("=" * 90)


# ============================================================
# DB HELPERS
# ============================================================
def fetch_table(db_path: Path, table_name: str) -> pd.DataFrame:
    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query(f'SELECT * FROM "{table_name}"', conn)
    finally:
        conn.close()
    return df


def ensure_output_tables() -> None:
    conn = sqlite3.connect(TRANSPORT_DB)
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS S_cl_DC_distance (
                origin_name TEXT NOT NULL,
                destination_name TEXT NOT NULL,
                distance_km REAL,
                PRIMARY KEY (origin_name, destination_name)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS S_cl_DC_time (
                origin_name TEXT NOT NULL,
                destination_name TEXT NOT NULL,
                time_min REAL,
                PRIMARY KEY (origin_name, destination_name)
            )
        """)
        conn.commit()
    finally:
        conn.close()


def upsert_distance_time(
    distance_rows: List[Tuple[str, str, Optional[float]]],
    time_rows: List[Tuple[str, str, Optional[float]]],
) -> None:
    conn = sqlite3.connect(TRANSPORT_DB)
    try:
        if OVERWRITE_EXISTING:
            conn.executemany("""
                INSERT INTO S_cl_DC_distance (origin_name, destination_name, distance_km)
                VALUES (?, ?, ?)
                ON CONFLICT(origin_name, destination_name)
                DO UPDATE SET distance_km = excluded.distance_km
            """, distance_rows)

            conn.executemany("""
                INSERT INTO S_cl_DC_time (origin_name, destination_name, time_min)
                VALUES (?, ?, ?)
                ON CONFLICT(origin_name, destination_name)
                DO UPDATE SET time_min = excluded.time_min
            """, time_rows)
        else:
            conn.executemany("""
                INSERT OR IGNORE INTO S_cl_DC_distance (origin_name, destination_name, distance_km)
                VALUES (?, ?, ?)
            """, distance_rows)

            conn.executemany("""
                INSERT OR IGNORE INTO S_cl_DC_time (origin_name, destination_name, time_min)
                VALUES (?, ?, ?)
            """, time_rows)

        conn.commit()
    finally:
        conn.close()


# ============================================================
# DATA LOADING
# ============================================================
def load_inputs() -> Tuple[pd.DataFrame, pd.DataFrame]:
    section("LOADING INPUT TABLES")

    df_f = fetch_table(MODEL_DB, "F")
    df_scl = fetch_table(MODEL_DB, "S_cl")

    required_f_cols = {"facility_name", "type", "latitude", "longitude"}
    required_scl_cols = {"origin", "latitude", "longitude"}

    missing_f = required_f_cols - set(df_f.columns)
    missing_scl = required_scl_cols - set(df_scl.columns)

    if missing_f:
        raise ValueError(f"F is missing columns: {sorted(missing_f)}")
    if missing_scl:
        raise ValueError(f"S_cl is missing columns: {sorted(missing_scl)}")

    df_dc = df_f[df_f["type"].astype(str).str.strip() == "DC"].copy()

    df_dc["facility_name"] = df_dc["facility_name"].astype(str).str.strip()
    df_scl["origin"] = df_scl["origin"].astype(str).str.strip()

    for col in ["latitude", "longitude"]:
        df_dc[col] = pd.to_numeric(df_dc[col], errors="coerce")
        df_scl[col] = pd.to_numeric(df_scl[col], errors="coerce")

    bad_dc = df_dc[df_dc["latitude"].isna() | df_dc["longitude"].isna()]
    bad_scl = df_scl[df_scl["latitude"].isna() | df_scl["longitude"].isna()]

    if not bad_dc.empty:
        raise ValueError(
            "Some DC rows have invalid coordinates:\n"
            + bad_dc[["facility_name", "latitude", "longitude"]].to_string(index=False)
        )

    if not bad_scl.empty:
        raise ValueError(
            "Some S_cl rows have invalid coordinates:\n"
            + bad_scl[["origin", "latitude", "longitude"]].to_string(index=False)
        )

    log(f"Loaded DC count   : {len(df_dc)}")
    log(f"Loaded S_cl count : {len(df_scl)}")

    print("\nDC preview:")
    print(df_dc[["facility_name", "latitude", "longitude"]].to_string(index=False))

    print("\nS_cl preview:")
    print(df_scl[["origin", "latitude", "longitude"]].to_string(index=False))

    return df_dc, df_scl


# ============================================================
# ORS HELPERS
# ============================================================
def ors_headers() -> dict:
    return {
        "Authorization": ORS_API_KEY,
        "Content-Type": "application/json",
    }


def request_matrix(
    origins: List[Tuple[str, float, float]],
    destinations: List[Tuple[str, float, float]],
    profile: str,
):
    url = f"{ORS_BASE_URL}/v2/matrix/{profile}"

    coordinates = []
    for _, lat, lon in origins:
        coordinates.append([float(lon), float(lat)])
    for _, lat, lon in destinations:
        coordinates.append([float(lon), float(lat)])

    payload = {
        "locations": coordinates,
        "sources": list(range(len(origins))),
        "destinations": list(range(len(origins), len(origins) + len(destinations))),
        "metrics": ["distance", "duration"],
        "units": "km",
    }

    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(url, json=payload, headers=ors_headers(), timeout=120)
            if resp.status_code != 200:
                raise RuntimeError(f"ORS HTTP {resp.status_code}: {resp.text}")
            return resp.json()
        except Exception as e:
            last_error = e
            log(f"[Matrix {profile}] attempt {attempt}/{MAX_RETRIES} failed: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_SLEEP_SEC)

    raise RuntimeError(f"Matrix request failed for profile {profile}: {last_error}")


def request_directions_pair(
    origin: Tuple[str, float, float],
    destination: Tuple[str, float, float],
    profile: str,
) -> Tuple[Optional[float], Optional[float]]:
    url = f"{ORS_BASE_URL}/v2/directions/{profile}"

    payload = {
        "coordinates": [
            [float(origin[2]), float(origin[1])],
            [float(destination[2]), float(destination[1])],
        ]
    }

    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(url, json=payload, headers=ors_headers(), timeout=120)
            if resp.status_code != 200:
                raise RuntimeError(f"ORS HTTP {resp.status_code}: {resp.text}")

            data = resp.json()
            routes = data.get("routes", [])
            if not routes:
                return None, None

            summary = routes[0].get("summary", {})
            dist_m = summary.get("distance")
            dur_s = summary.get("duration")

            if dist_m is None or dur_s is None:
                return None, None

            return float(dist_m) / 1000.0, float(dur_s) / 60.0

        except Exception as e:
            last_error = e
            log(
                f"[Directions {profile}] {origin[0]} -> {destination[0]} "
                f"attempt {attempt}/{MAX_RETRIES} failed: {e}"
            )
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_SLEEP_SEC)

    log(f"[Directions {profile}] final failure for {origin[0]} -> {destination[0]}: {last_error}")
    return None, None


# ============================================================
# CORE LOGIC
# ============================================================
def build_rows_with_fallback(
    origins: List[Tuple[str, float, float]],
    destinations: List[Tuple[str, float, float]],
) -> Tuple[List[Tuple[str, str, Optional[float]]], List[Tuple[str, str, Optional[float]]]]:
    section("REQUESTING PRIMARY MATRIX")

    matrix_data = request_matrix(origins, destinations, PRIMARY_PROFILE)
    distances = matrix_data.get("distances")
    durations = matrix_data.get("durations")

    if distances is None or durations is None:
        raise RuntimeError(f"Primary matrix response missing distances/durations: {matrix_data}")

    distance_rows = []
    time_rows = []
    null_pairs = []

    for i, origin in enumerate(origins):
        for j, destination in enumerate(destinations):
            dist_km = distances[i][j]
            dur_sec = durations[i][j]

            if dist_km is None or dur_sec is None:
                null_pairs.append((origin, destination))
                continue

            distance_rows.append((origin[0], destination[0], float(dist_km)))
            time_rows.append((origin[0], destination[0], float(dur_sec) / 60.0))

    log(f"Primary matrix resolved pairs : {len(distance_rows)}")
    log(f"Primary matrix null pairs     : {len(null_pairs)}")

    if null_pairs:
        section("RESOLVING NULL PAIRS WITH FALLBACK DIRECTIONS")

    for origin, destination in null_pairs:
        log(f"Trying fallback for {origin[0]} -> {destination[0]}")

        # First try same pair with truck profile as individual directions
        dist_km, time_min = request_directions_pair(origin, destination, PRIMARY_PROFILE)

        used_profile = PRIMARY_PROFILE

        # Then fallback to car if truck profile still fails
        if dist_km is None or time_min is None:
            dist_km, time_min = request_directions_pair(origin, destination, FALLBACK_PROFILE)
            used_profile = FALLBACK_PROFILE

        if dist_km is None or time_min is None:
            log(f"[WARNING] Could not resolve pair {origin[0]} -> {destination[0]}; storing NULL")
            distance_rows.append((origin[0], destination[0], None))
            time_rows.append((origin[0], destination[0], None))
        else:
            log(
                f"Resolved {origin[0]} -> {destination[0]} "
                f"with profile={used_profile} | dist_km={dist_km:.2f} | time_min={time_min:.2f}"
            )
            distance_rows.append((origin[0], destination[0], dist_km))
            time_rows.append((origin[0], destination[0], time_min))

        time.sleep(REQUEST_SLEEP_SEC)

    return distance_rows, time_rows


# ============================================================
# MAIN
# ============================================================
def main():
    section("BUILD S_cl -> DC MATRICES WITH ORS (ROBUST)")

    if not ORS_API_KEY:
        raise ValueError("ORS_API_KEY is not set")

    ensure_output_tables()
    df_dc, df_scl = load_inputs()

    origins = [
        (row["origin"], float(row["latitude"]), float(row["longitude"]))
        for _, row in df_scl.iterrows()
    ]
    destinations = [
        (row["facility_name"], float(row["latitude"]), float(row["longitude"]))
        for _, row in df_dc.iterrows()
    ]

    log(f"Origins      : {len(origins)}")
    log(f"Destinations : {len(destinations)}")
    log(f"Pairs        : {len(origins) * len(destinations)}")
    log(f"Primary      : {PRIMARY_PROFILE}")
    log(f"Fallback     : {FALLBACK_PROFILE}")

    distance_rows, time_rows = build_rows_with_fallback(origins, destinations)

    section("WRITING RESULTS TO SQLITE")
    upsert_distance_time(distance_rows, time_rows)

    distance_df = pd.DataFrame(distance_rows, columns=["origin_name", "destination_name", "distance_km"])
    time_df = pd.DataFrame(time_rows, columns=["origin_name", "destination_name", "time_min"])

    print("\nDistance preview:")
    print(distance_df.to_string(index=False))

    print("\nTime preview:")
    print(time_df.to_string(index=False))

    null_dist = distance_df["distance_km"].isna().sum()
    null_time = time_df["time_min"].isna().sum()

    log(f"\nNull distances stored: {null_dist}")
    log(f"Null times stored    : {null_time}")

    section("DONE")
    log("Updated:")
    log("- Sets/transport_matrices.db :: S_cl_DC_distance")
    log("- Sets/transport_matrices.db :: S_cl_DC_time")


if __name__ == "__main__":
    main()