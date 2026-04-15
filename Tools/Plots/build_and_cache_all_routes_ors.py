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
OUTPUT_DIR = Path("Tools/Plots/routes")
FAILED_LOG_PATH = OUTPUT_DIR / "failed_routes.csv"

ORS_API_KEY = os.getenv("ORS_API_KEY", "").strip()
ORS_BASE_URL = "https://api.openrouteservice.org"
ORS_PROFILE = "driving-hgv"

OVERWRITE = False
REQUEST_TIMEOUT_SEC = 120
SLEEP_BETWEEN_CALLS_SEC = 1.2
MAX_RETRIES = 4

DIR_E_TO_DC = OUTPUT_DIR / "E_to_DC"
DIR_SCL_TO_DC = OUTPUT_DIR / "S_cl_to_DC"
DIR_WM_TO_DC = OUTPUT_DIR / "WM_to_DC"
DIR_DC_TO_DS = OUTPUT_DIR / "DC_to_DS"


# ======================================================================================
# BASIC HELPERS
# ======================================================================================

def banner(text: str) -> None:
    print("\n" + "=" * 90)
    print(text)
    print("=" * 90)


def subbanner(text: str) -> None:
    print("\n" + "-" * 90)
    print(text)
    print("-" * 90)


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


def quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


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


def lonlat(node: Dict) -> Tuple[float, float]:
    return (float(node["lon"]), float(node["lat"]))


def append_failed_log(relation: str, origin: str, destination: str, error_msg: str) -> None:
    ensure_dir(FAILED_LOG_PATH.parent)
    write_header = not FAILED_LOG_PATH.exists()

    with open(FAILED_LOG_PATH, "a", encoding="utf-8", newline="") as f:
        if write_header:
            f.write("relation,origin,destination,error\n")

        safe_error = str(error_msg).replace("\n", " ").replace(",", ";")
        f.write(f"{relation},{origin},{destination},{safe_error}\n")


# ======================================================================================
# LOAD NODES
# ======================================================================================

def load_nodes(conn: sqlite3.Connection) -> Dict[str, Dict]:
    banner("LOADING NODES")

    node_map: Dict[str, Dict] = {}

    # F: DC, DS, MDCP
    rows_f = fetch(
        conn,
        """
        SELECT facility_name, type, latitude, longitude
        FROM F
        """
    )
    print(f"Facilities loaded: {len(rows_f)}")

    for r in rows_f:
        name = str(r["facility_name"]).strip()
        node_map[name] = {
            "name": name,
            "kind": str(r["type"]).strip(),
            "lat": float(r["latitude"]),
            "lon": float(r["longitude"]),
            "source_table": "F",
        }

    # E
    rows_e = fetch(
        conn,
        """
        SELECT "international entry point", latitude, longitude
        FROM E
        """
    )
    print(f"E loaded: {len(rows_e)}")

    for r in rows_e:
        name = str(r["international entry point"]).strip()
        node_map[name] = {
            "name": name,
            "kind": "E",
            "lat": float(r["latitude"]),
            "lon": float(r["longitude"]),
            "source_table": "E",
        }

    # S_cl
    rows_scl = fetch(
        conn,
        """
        SELECT origin, latitude, longitude
        FROM S_cl
        """
    )
    print(f"S_cl loaded: {len(rows_scl)}")

    for r in rows_scl:
        name = str(r["origin"]).strip()
        node_map[name] = {
            "name": name,
            "kind": "S_cl",
            "lat": float(r["latitude"]),
            "lon": float(r["longitude"]),
            "source_table": "S_cl",
        }

    # WM
    rows_wm = fetch(
        conn,
        """
        SELECT wholesale_market, latitude, longitude
        FROM WM
        """
    )
    print(f"WM loaded: {len(rows_wm)}")

    for r in rows_wm:
        name = str(r["wholesale_market"]).strip()
        node_map[name] = {
            "name": name,
            "kind": "WM",
            "lat": float(r["latitude"]),
            "lon": float(r["longitude"]),
            "source_table": "WM",
        }

    print(f"Total nodes: {len(node_map)}")

    print(f"DC   : {len([v for v in node_map.values() if v['kind'] == 'DC'])}")
    print(f"DS   : {len([v for v in node_map.values() if v['kind'] == 'DS'])}")
    print(f"MDCP : {len([v for v in node_map.values() if v['kind'] == 'MDCP'])}")
    print(f"E    : {len([v for v in node_map.values() if v['kind'] == 'E'])}")
    print(f"S_cl : {len([v for v in node_map.values() if v['kind'] == 'S_cl'])}")
    print(f"WM   : {len([v for v in node_map.values() if v['kind'] == 'WM'])}")

    return node_map


def get_nodes_by_kind(node_map: Dict[str, Dict], kind: str) -> List[Dict]:
    return [v for v in node_map.values() if v["kind"] == kind]


# ======================================================================================
# ORS DIRECTIONS VIA REQUESTS
# ======================================================================================

def call_directions_api(
    origin_lon: float,
    origin_lat: float,
    destination_lon: float,
    destination_lat: float,
    profile: str = ORS_PROFILE,
) -> Dict:
    url = f"{ORS_BASE_URL}/v2/directions/{profile}/geojson"

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

    geometry_dict = features[0].get("geometry")
    if not geometry_dict:
        raise RuntimeError("Directions API devolvió feature sin geometry.")

    summary = features[0].get("properties", {}).get("summary", {})

    return {
        "geometry": geometry_dict,
        "distance_m": summary.get("distance"),
        "duration_s": summary.get("duration"),
        "raw": data,
    }


def call_directions_api_with_retry(
    origin_lon: float,
    origin_lat: float,
    destination_lon: float,
    destination_lat: float,
    profile: str = ORS_PROFILE,
    max_retries: int = MAX_RETRIES,
) -> Dict:
    last_error = None

    for attempt in range(max_retries):
        try:
            return call_directions_api(
                origin_lon=origin_lon,
                origin_lat=origin_lat,
                destination_lon=destination_lon,
                destination_lat=destination_lat,
                profile=profile,
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


# ======================================================================================
# ROUTING CORE
# ======================================================================================

def route_pair(
    origin: Dict,
    destination: Dict,
    folder: Path,
    relation: str,
) -> bool:
    filename = f"{sanitize(origin['name'])}_{sanitize(destination['name'])}.geojson"
    path = folder / filename

    if path.exists() and not OVERWRITE:
        print(f"SKIP {filename}")
        return False

    result = call_directions_api_with_retry(
        origin_lon=float(origin["lon"]),
        origin_lat=float(origin["lat"]),
        destination_lon=float(destination["lon"]),
        destination_lat=float(destination["lat"]),
        profile=ORS_PROFILE,
        max_retries=MAX_RETRIES,
    )

    geojson_feature = {
        "type": "Feature",
        "geometry": result["geometry"],
        "properties": {
            "relation": relation,
            "profile": ORS_PROFILE,
            "origin": origin["name"],
            "origin_kind": origin["kind"],
            "destination": destination["name"],
            "destination_kind": destination["kind"],
            "distance_m": result["distance_m"],
            "duration_s": result["duration_s"],
        },
    }

    save_geojson(geojson_feature, path)
    print(f"OK {filename}")

    time.sleep(SLEEP_BETWEEN_CALLS_SEC)
    return True


def run_relation(
    relation_name: str,
    origins: List[Dict],
    destinations: List[Dict],
    output_dir: Path,
) -> None:
    banner(relation_name)

    ensure_dir(output_dir)

    total = len(origins) * len(destinations)
    counter = 0
    ok_count = 0
    fail_count = 0
    skip_count = 0

    for origin in origins:
        for destination in destinations:
            counter += 1
            print(f"[{counter}/{total}] {origin['name']} -> {destination['name']}")

            try:
                created = route_pair(
                    origin=origin,
                    destination=destination,
                    folder=output_dir,
                    relation=relation_name,
                )
                if created:
                    ok_count += 1
                else:
                    skip_count += 1
            except Exception as exc:
                fail_count += 1
                print(f"FAIL {origin['name']} -> {destination['name']} :: {exc}")
                append_failed_log(
                    relation=relation_name,
                    origin=origin["name"],
                    destination=destination["name"],
                    error_msg=str(exc),
                )

    print("\nResumen")
    print(f"Created : {ok_count}")
    print(f"Skipped : {skip_count}")
    print(f"Failed  : {fail_count}")


# ======================================================================================
# MAIN
# ======================================================================================

def main() -> None:
    banner("BUILD ROUTES CACHE")

    validate_api_key()
    ensure_dir(OUTPUT_DIR)

    conn = connect_db(MODEL_DB_PATH)
    try:
        nodes = load_nodes(conn)

        e_nodes = get_nodes_by_kind(nodes, "E")
        scl_nodes = get_nodes_by_kind(nodes, "S_cl")
        wm_nodes = get_nodes_by_kind(nodes, "WM")
        dc_nodes = get_nodes_by_kind(nodes, "DC")
        ds_nodes = get_nodes_by_kind(nodes, "DS")

        run_relation(
            relation_name="E_to_DC",
            origins=e_nodes,
            destinations=dc_nodes,
            output_dir=DIR_E_TO_DC,
        )

        run_relation(
            relation_name="S_cl_to_DC",
            origins=scl_nodes,
            destinations=dc_nodes,
            output_dir=DIR_SCL_TO_DC,
        )

        run_relation(
            relation_name="WM_to_DC",
            origins=wm_nodes,
            destinations=dc_nodes,
            output_dir=DIR_WM_TO_DC,
        )

        run_relation(
            relation_name="DC_to_DS",
            origins=dc_nodes,
            destinations=ds_nodes,
            output_dir=DIR_DC_TO_DS,
        )

    finally:
        conn.close()

    banner("DONE")
    print(f"Output dir : {OUTPUT_DIR.resolve()}")
    print(f"Failed log : {FAILED_LOG_PATH.resolve()}")


if __name__ == "__main__":
    main()