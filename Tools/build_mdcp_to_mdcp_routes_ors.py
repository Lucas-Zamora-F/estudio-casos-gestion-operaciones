from __future__ import annotations

import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Dict, List, Tuple

import requests


# ======================================================================================
# CONFIG
# ======================================================================================

MODEL_DB_PATH = Path("Sets/model.db")
OUTPUT_DIR = Path("Tools/Plots/routes/MDCP_to_MDCP")
FAILED_LOG_PATH = OUTPUT_DIR / "failed_routes.csv"

ORS_API_KEY = os.getenv("ORS_API_KEY", "").strip()
ORS_BASE_URL = "https://api.openrouteservice.org"
ORS_PROFILE = "driving-hgv"

OVERWRITE = False
INCLUDE_SELF_LOOPS = False
REQUEST_TIMEOUT_SEC = 120
SLEEP_BETWEEN_CALLS_SEC = 1.2
MAX_RETRIES = 4


# ======================================================================================
# HELPERS
# ======================================================================================

def banner(text: str) -> None:
    print("\n" + "=" * 90)
    print(text)
    print("=" * 90)


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def validate_api_key() -> None:
    if not ORS_API_KEY:
        raise ValueError("Debes definir ORS_API_KEY en variables de entorno.")


def build_headers() -> dict[str, str]:
    return {
        "Authorization": ORS_API_KEY,
        "Content-Type": "application/json; charset=utf-8",
        "Accept": "application/json, application/geo+json",
    }


def sanitize(text: str) -> str:
    text = str(text).strip()
    replacements = {
        " ": "_",
        "/": "_",
        "\\": "_",
        ":": "_",
        ";": "_",
        ",": "_",
        ".": "_",
        "(": "",
        ")": "",
        "[": "",
        "]": "",
        "{": "",
        "}": "",
        "'": "",
        '"': "",
        "|": "_",
        "?": "",
        "!": "",
        "*": "_",
        "<": "_",
        ">": "_",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)

    while "__" in text:
        text = text.replace("__", "_")

    return text.strip("_")


def connect_db(path: Path) -> sqlite3.Connection:
    if not path.exists():
        raise FileNotFoundError(f"No existe DB: {path}")
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def fetch(conn: sqlite3.Connection, query: str) -> List[sqlite3.Row]:
    cur = conn.cursor()
    cur.execute(query)
    return cur.fetchall()


def append_failed_log(origin: str, destination: str, error_msg: str) -> None:
    ensure_dir(FAILED_LOG_PATH.parent)
    write_header = not FAILED_LOG_PATH.exists()

    with open(FAILED_LOG_PATH, "a", encoding="utf-8", newline="") as f:
        if write_header:
            f.write("origin,destination,error\n")

        safe_error = str(error_msg).replace("\n", " ").replace(",", ";")
        f.write(f"{origin},{destination},{safe_error}\n")


# ======================================================================================
# LOAD MDCP
# ======================================================================================

def load_mdcp_nodes(conn: sqlite3.Connection) -> List[Dict]:
    banner("LOADING MDCP NODES")

    rows = fetch(
        conn,
        """
        SELECT facility_name, type, latitude, longitude
        FROM F
        WHERE type = 'MDCP'
        ORDER BY facility_name
        """
    )

    if not rows:
        raise RuntimeError("No se encontraron filas type='MDCP' en tabla F.")

    nodes = []
    for r in rows:
        nodes.append(
            {
                "name": str(r["facility_name"]).strip(),
                "kind": "MDCP",
                "lat": float(r["latitude"]),
                "lon": float(r["longitude"]),
            }
        )

    print(f"MDCP encontrados: {len(nodes)}")
    for n in nodes:
        print(f" - {n['name']} ({n['lat']:.6f}, {n['lon']:.6f})")

    return nodes


# ======================================================================================
# ORS
# ======================================================================================

def call_directions_api(
    origin_lon: float,
    origin_lat: float,
    destination_lon: float,
    destination_lat: float,
) -> Dict:
    url = f"{ORS_BASE_URL}/v2/directions/{ORS_PROFILE}/geojson"

    payload = {
        "coordinates": [
            [float(origin_lon), float(origin_lat)],
            [float(destination_lon), float(destination_lat)],
        ],
        "radiuses": [-1, -1],
        "instructions": False,
        "geometry_simplify": False,
    }

    response = requests.post(
        url,
        headers=build_headers(),
        json=payload,
        timeout=REQUEST_TIMEOUT_SEC,
    )

    if response.status_code != 200:
        raise RuntimeError(
            f"Directions API error {response.status_code}: {response.text}"
        )

    data = response.json()
    features = data.get("features", [])

    if not features:
        raise RuntimeError("Directions API devolvió 0 features.")

    feature = features[0]
    geometry_dict = feature.get("geometry")
    if not geometry_dict:
        raise RuntimeError("Directions API devolvió feature sin geometry.")

    summary = feature.get("properties", {}).get("summary", {})

    return {
        "geometry": geometry_dict,
        "distance_m": summary.get("distance"),
        "duration_s": summary.get("duration"),
    }


def call_directions_api_with_retry(
    origin_lon: float,
    origin_lat: float,
    destination_lon: float,
    destination_lat: float,
) -> Dict:
    last_error = None

    for attempt in range(MAX_RETRIES):
        try:
            return call_directions_api(
                origin_lon=origin_lon,
                origin_lat=origin_lat,
                destination_lon=destination_lon,
                destination_lat=destination_lat,
            )
        except Exception as exc:
            last_error = exc
            error_text = str(exc)

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

    raise RuntimeError(
        f"Max retries exceeded for route "
        f"({origin_lat}, {origin_lon}) -> ({destination_lat}, {destination_lon}): {last_error}"
    )


# ======================================================================================
# SAVE
# ======================================================================================

def save_geojson(obj: Dict, path: Path) -> None:
    ensure_dir(path.parent)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)


def route_pair(origin: Dict, destination: Dict) -> bool:
    filename = f"{sanitize(origin['name'])}_{sanitize(destination['name'])}.geojson"
    path = OUTPUT_DIR / filename

    if path.exists() and not OVERWRITE:
        print(f"SKIP {filename}")
        return False

    result = call_directions_api_with_retry(
        origin_lon=float(origin["lon"]),
        origin_lat=float(origin["lat"]),
        destination_lon=float(destination["lon"]),
        destination_lat=float(destination["lat"]),
    )

    feature = {
        "type": "Feature",
        "geometry": result["geometry"],
        "properties": {
            "relation": "MDCP_to_MDCP",
            "profile": ORS_PROFILE,
            "origin": origin["name"],
            "origin_kind": "MDCP",
            "destination": destination["name"],
            "destination_kind": "MDCP",
            "distance_m": result["distance_m"],
            "duration_s": result["duration_s"],
        },
    }

    save_geojson(feature, path)
    print(f"OK {filename}")

    time.sleep(SLEEP_BETWEEN_CALLS_SEC)
    return True


# ======================================================================================
# MAIN
# ======================================================================================

def main() -> None:
    banner("BUILD MDCP -> MDCP ROUTES CACHE")

    validate_api_key()
    ensure_dir(OUTPUT_DIR)

    conn = connect_db(MODEL_DB_PATH)
    try:
        mdcp_nodes = load_mdcp_nodes(conn)
    finally:
        conn.close()

    total = 0
    created = 0
    skipped = 0
    failed = 0

    for origin in mdcp_nodes:
        for destination in mdcp_nodes:
            if not INCLUDE_SELF_LOOPS and origin["name"] == destination["name"]:
                continue

            total += 1
            print(f"[{total}] {origin['name']} -> {destination['name']}")

            try:
                was_created = route_pair(origin, destination)
                if was_created:
                    created += 1
                else:
                    skipped += 1
            except Exception as exc:
                failed += 1
                print(f"FAIL {origin['name']} -> {destination['name']} :: {exc}")
                append_failed_log(
                    origin=origin["name"],
                    destination=destination["name"],
                    error_msg=str(exc),
                )

    banner("DONE")
    print(f"Total   : {total}")
    print(f"Created : {created}")
    print(f"Skipped : {skipped}")
    print(f"Failed  : {failed}")
    print(f"Output  : {OUTPUT_DIR.resolve()}")


if __name__ == "__main__":
    main()