from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Dict, List, Any

import pandas as pd


# ============================================================
# CONFIG
# ============================================================
BASE_RESULTS_DIR = Path("main/optimization_results")
MODEL_DB = Path("Sets/model.db")

GRAPH_DIR_NAME = "graph"
PLOT_CONTEXT_FILENAME = "plot_context.json"

VALID_SUPPLY_TO_DC_FLOW_TYPES = {"E_to_DC", "Scl_to_DC", "WM_to_DC"}

# Transportation costs [USD / kg / km]
C_TR_LONG = {
    "Roma Tomatoes": 0.0010,
    "Cauliflower": 0.0010,
    "Broccoli": 0.0010,
    "Asparagus": 0.0012,
    "Green Bell Pepper": 0.0010,
}

C_TR_INT = {
    "Roma Tomatoes": 0.0012,
    "Cauliflower": 0.0012,
    "Broccoli": 0.0012,
    "Asparagus": 0.0014,
    "Green Bell Pepper": 0.0012,
}

C_LM = {
    "Roma Tomatoes": 0.0030,
    "Cauliflower": 0.0030,
    "Broccoli": 0.0030,
    "Asparagus": 0.0033,
    "Green Bell Pepper": 0.0030,
}

C_MOV = 0.90

MONTH_ORDER = {
    "Jan": 1,
    "Feb": 2,
    "Mar": 3,
    "Apr": 4,
    "May": 5,
    "Jun": 6,
    "Jul": 7,
    "Aug": 8,
    "Sep": 9,
    "Oct": 10,
    "Nov": 11,
    "Dec": 12,
}


# ============================================================
# LOGGING
# ============================================================
def log(msg: str) -> None:
    now = time.strftime("%H:%M:%S")
    print(f"[{now}] {msg}")


def section(title: str) -> None:
    print("\n" + "=" * 90)
    print(title)
    print("=" * 90)


# ============================================================
# BASIC HELPERS
# ============================================================
def normalize_text(x) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip()


def safe_float(x, default: float = 0.0) -> float:
    try:
        if pd.isna(x):
            return default
        return float(x)
    except Exception:
        return default


def safe_read_csv(path: Path) -> pd.DataFrame:
    try:
        if not path.exists() or not path.is_file():
            return pd.DataFrame()

        if path.stat().st_size == 0:
            return pd.DataFrame()

        df = pd.read_csv(path)

        for col in df.columns:
            if pd.api.types.is_object_dtype(df[col]):
                df[col] = df[col].apply(normalize_text)

        return df

    except pd.errors.EmptyDataError:
        return pd.DataFrame()
    except UnicodeDecodeError:
        try:
            df = pd.read_csv(path, encoding="latin1")
            for col in df.columns:
                if pd.api.types.is_object_dtype(df[col]):
                    df[col] = df[col].apply(normalize_text)
            return df
        except Exception as e:
            log(f"[WARNING] No se pudo leer {path} con latin1: {e}")
            return pd.DataFrame()
    except Exception as e:
        log(f"[WARNING] No se pudo leer {path}: {e}")
        return pd.DataFrame()


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"No existe el archivo JSON: {path.resolve()}")

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def fetch_table(db_path: Path, table_name: str) -> pd.DataFrame:
    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query(f'SELECT * FROM "{table_name}"', conn)
        for col in df.columns:
            if pd.api.types.is_object_dtype(df[col]):
                df[col] = df[col].apply(normalize_text)
        return df
    finally:
        conn.close()


def month_sort_key(x: str) -> int:
    return MONTH_ORDER.get(normalize_text(x), 999)


def ordered_unique_months(*dfs: pd.DataFrame) -> List[str]:
    values: List[str] = []

    for df in dfs:
        if not df.empty and "month" in df.columns:
            values.extend(df["month"].dropna().astype(str).tolist())

    if not values:
        return []

    unique_months = list(dict.fromkeys(values))
    return sorted(unique_months, key=month_sort_key)


def normalize_flow_type(flow_type: str) -> str:
    flow_type = normalize_text(flow_type)

    if flow_type == "S_cl_to_DC":
        return "Scl_to_DC"

    return flow_type


def get_transport_rate(flow_type: str, product: str) -> float:
    flow_type = normalize_flow_type(flow_type)
    product = normalize_text(product)

    if flow_type == "E_to_DC":
        return safe_float(C_TR_LONG.get(product, 0.0))

    if flow_type in {"Scl_to_DC", "WM_to_DC", "DC_to_DS", "DC_to_MDCP"}:
        return safe_float(C_TR_INT.get(product, 0.0))

    if flow_type == "F_to_C":
        return safe_float(C_LM.get(product, 0.0))

    return 0.0


def infer_origin_country_from_row(
    flow_type: str,
    origin_name: str,
    purchase_origin: str,
) -> str:
    flow_type = normalize_flow_type(flow_type)
    origin_name = normalize_text(origin_name)
    purchase_origin = normalize_text(purchase_origin)

    if flow_type in {"Scl_to_DC", "WM_to_DC"}:
        return "Chile"

    if flow_type == "E_to_DC":
        if purchase_origin:
            return purchase_origin
        return "Unknown"

    return ""


# ============================================================
# LOAD PLOT CONTEXTS
# ============================================================
def find_plot_context_files(base_results_dir: Path = BASE_RESULTS_DIR) -> List[Path]:
    return sorted(base_results_dir.glob(f"*/{GRAPH_DIR_NAME}/{PLOT_CONTEXT_FILENAME}"))


def load_model_contexts(base_results_dir: Path = BASE_RESULTS_DIR) -> List[Dict[str, Any]]:
    section("LOADING MODEL PLOT CONTEXTS")

    context_paths = find_plot_context_files(base_results_dir)

    if not context_paths:
        raise FileNotFoundError(
            f"No se encontraron archivos {PLOT_CONTEXT_FILENAME} en "
            f"{base_results_dir.resolve()}"
        )

    contexts = []
    for path in context_paths:
        ctx = load_json(path)
        ctx["_plot_context_path"] = str(path.resolve())
        contexts.append(ctx)
        log(f"Loaded context: {path}")

    log(f"Total model contexts: {len(contexts)}")
    return contexts


# ============================================================
# LOAD ONLY THE DB TABLES WE NEED FOR SUPPLIER -> DC
# ============================================================
def load_reference_tables(model_db_path: Path = MODEL_DB) -> Dict[str, pd.DataFrame]:
    section("LOADING REFERENCE TABLES FOR SUPPLIER -> DC")

    if not model_db_path.exists():
        raise FileNotFoundError(f"No existe model.db: {model_db_path.resolve()}")

    df_E = fetch_table(model_db_path, "E")
    df_F = fetch_table(model_db_path, "F")
    df_S_cl = fetch_table(model_db_path, "S_cl")
    df_WM = fetch_table(model_db_path, "WM")

    log(f"E rows   : {len(df_E)}")
    log(f"F rows   : {len(df_F)}")
    log(f"S_cl rows: {len(df_S_cl)}")
    log(f"WM rows  : {len(df_WM)}")

    return {
        "E": df_E,
        "F": df_F,
        "S_cl": df_S_cl,
        "WM": df_WM,
    }


def build_reference_lookups(
    reference_tables: Dict[str, pd.DataFrame],
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    section("BUILDING REFERENCE LOOKUPS")

    df_E = reference_tables["E"]
    df_F = reference_tables["F"]
    df_S_cl = reference_tables["S_cl"]
    df_WM = reference_tables["WM"]

    lookup_E: Dict[str, Dict[str, Any]] = {}
    lookup_F: Dict[str, Dict[str, Any]] = {}
    lookup_S_cl: Dict[str, Dict[str, Any]] = {}
    lookup_WM: Dict[str, Dict[str, Any]] = {}

    for _, row in df_E.iterrows():
        name = normalize_text(row["international entry point"])
        lookup_E[name] = {
            "name": name,
            "node_category": "E",
            "type": normalize_text(row.get("type", "E")),
            "lat": safe_float(row["latitude"]),
            "lon": safe_float(row["longitude"]),
        }

    for _, row in df_F.iterrows():
        name = normalize_text(row["facility_name"])
        lookup_F[name] = {
            "name": name,
            "node_category": "F",
            "type": normalize_text(row.get("type", "")),
            "lat": safe_float(row["latitude"]),
            "lon": safe_float(row["longitude"]),
        }

    for _, row in df_S_cl.iterrows():
        name = normalize_text(row["origin"])
        lookup_S_cl[name] = {
            "name": name,
            "node_category": "S_cl",
            "type": "S_cl",
            "lat": safe_float(row["latitude"]),
            "lon": safe_float(row["longitude"]),
        }

    for _, row in df_WM.iterrows():
        name = normalize_text(row["wholesale_market"])
        lookup_WM[name] = {
            "name": name,
            "node_category": "WM",
            "type": "WM",
            "lat": safe_float(row["latitude"]),
            "lon": safe_float(row["longitude"]),
        }

    log(f"Lookup E    : {len(lookup_E)}")
    log(f"Lookup F    : {len(lookup_F)}")
    log(f"Lookup S_cl : {len(lookup_S_cl)}")
    log(f"Lookup WM   : {len(lookup_WM)}")

    return {
        "E": lookup_E,
        "F": lookup_F,
        "S_cl": lookup_S_cl,
        "WM": lookup_WM,
    }


def get_origin_info(
    origin_name: str,
    lookups: Dict[str, Dict[str, Dict[str, Any]]],
) -> Dict[str, Any] | None:
    origin_name = normalize_text(origin_name)

    for group_name in ["E", "S_cl", "WM"]:
        if origin_name in lookups[group_name]:
            return lookups[group_name][origin_name]
    return None


def get_destination_info(
    destination_name: str,
    lookups: Dict[str, Dict[str, Dict[str, Any]]],
) -> Dict[str, Any] | None:
    destination_name = normalize_text(destination_name)
    return lookups["F"].get(destination_name)


# ============================================================
# MODEL CSV LOADING
# ============================================================
def load_model_csvs(model_context: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
    csv_paths = model_context["csvs"]

    df_summary = safe_read_csv(Path(csv_paths["summary.csv"]["path"]))
    df_flows = safe_read_csv(Path(csv_paths["flows.csv"]["path"]))
    df_purchases = safe_read_csv(Path(csv_paths["purchases.csv"]["path"]))
    df_deliveries = safe_read_csv(Path(csv_paths["deliveries.csv"]["path"])) if "deliveries.csv" in csv_paths else pd.DataFrame()
    df_open_facilities = safe_read_csv(Path(csv_paths["open_facilities.csv"]["path"])) if "open_facilities.csv" in csv_paths else pd.DataFrame()
    df_mdc_assignment = safe_read_csv(Path(csv_paths["mdc_assignment.csv"]["path"])) if "mdc_assignment.csv" in csv_paths else pd.DataFrame()
    df_config_selection = safe_read_csv(Path(csv_paths["config_selection.csv"]["path"])) if "config_selection.csv" in csv_paths else pd.DataFrame()

    return {
        "summary": df_summary,
        "flows": df_flows,
        "purchases": df_purchases,
        "deliveries": df_deliveries,
        "open_facilities": df_open_facilities,
        "mdc_assignment": df_mdc_assignment,
        "config_selection": df_config_selection,
    }


# ============================================================
# PURCHASE AGGREGATION FOR ENRICHMENT
# ============================================================
def build_purchase_aggregates(df_purchases: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    if df_purchases.empty:
        empty = pd.DataFrame()
        return {
            "import_by_entry_product_month": empty,
            "domestic_by_origin_product_month": empty,
            "wm_by_origin_product_month": empty,
        }

    purchases = df_purchases.copy()

    if "purchase_type" in purchases.columns:
        purchases["purchase_type"] = purchases["purchase_type"].apply(normalize_text)

    if "product" in purchases.columns:
        purchases["product"] = purchases["product"].apply(normalize_text)

    if "origin" in purchases.columns:
        purchases["origin"] = purchases["origin"].apply(normalize_text)

    if "entry_point" in purchases.columns:
        purchases["entry_point"] = purchases["entry_point"].apply(normalize_text)

    if "month" in purchases.columns:
        purchases["month"] = purchases["month"].apply(normalize_text)

    if "quantity_kg" in purchases.columns:
        purchases["quantity_kg"] = purchases["quantity_kg"].apply(safe_float)

    if "total_purchase_cost_usd" in purchases.columns:
        purchases["total_purchase_cost_usd"] = purchases["total_purchase_cost_usd"].apply(safe_float)

    df_import = purchases[purchases["purchase_type"] == "import"].copy()
    df_domestic = purchases[purchases["purchase_type"] == "domestic"].copy()
    df_wm = purchases[purchases["purchase_type"] == "wholesale_market"].copy()

    agg_import = pd.DataFrame()
    if not df_import.empty:
        agg_import = (
            df_import.groupby(
                ["month", "product", "entry_point", "origin"],
                as_index=False
            )
            .agg(
                purchase_quantity_kg=("quantity_kg", "sum"),
                purchase_total_cost_usd=("total_purchase_cost_usd", "sum"),
            )
        )
        agg_import["purchase_unit_cost_usd_per_kg"] = agg_import.apply(
            lambda r: safe_float(r["purchase_total_cost_usd"]) / safe_float(r["purchase_quantity_kg"])
            if safe_float(r["purchase_quantity_kg"]) > 0
            else 0.0,
            axis=1,
        )

    agg_domestic = pd.DataFrame()
    if not df_domestic.empty:
        agg_domestic = (
            df_domestic.groupby(
                ["month", "product", "origin"],
                as_index=False
            )
            .agg(
                purchase_quantity_kg=("quantity_kg", "sum"),
                purchase_total_cost_usd=("total_purchase_cost_usd", "sum"),
            )
        )
        agg_domestic["purchase_unit_cost_usd_per_kg"] = agg_domestic.apply(
            lambda r: safe_float(r["purchase_total_cost_usd"]) / safe_float(r["purchase_quantity_kg"])
            if safe_float(r["purchase_quantity_kg"]) > 0
            else 0.0,
            axis=1,
        )

    agg_wm = pd.DataFrame()
    if not df_wm.empty:
        agg_wm = (
            df_wm.groupby(
                ["month", "product", "origin"],
                as_index=False
            )
            .agg(
                purchase_quantity_kg=("quantity_kg", "sum"),
                purchase_total_cost_usd=("total_purchase_cost_usd", "sum"),
            )
        )
        agg_wm["purchase_unit_cost_usd_per_kg"] = agg_wm.apply(
            lambda r: safe_float(r["purchase_total_cost_usd"]) / safe_float(r["purchase_quantity_kg"])
            if safe_float(r["purchase_quantity_kg"]) > 0
            else 0.0,
            axis=1,
        )

    return {
        "import_by_entry_product_month": agg_import,
        "domestic_by_origin_product_month": agg_domestic,
        "wm_by_origin_product_month": agg_wm,
    }


def resolve_purchase_info_for_connection(
    flow_type: str,
    month: str,
    product: str,
    origin: str,
    quantity_kg: float,
    purchase_aggs: Dict[str, pd.DataFrame],
) -> Dict[str, Any]:
    flow_type = normalize_flow_type(flow_type)
    month = normalize_text(month)
    product = normalize_text(product)
    origin = normalize_text(origin)
    quantity_kg = safe_float(quantity_kg)

    result = {
        "purchase_origin": "",
        "purchase_quantity_kg_ref": None,
        "purchase_unit_cost_usd_per_kg": 0.0,
        "purchase_cost_usd": 0.0,
    }

    if flow_type == "E_to_DC":
        df = purchase_aggs["import_by_entry_product_month"]
        if df.empty:
            return result

        tmp = df[
            (df["month"] == month)
            & (df["product"] == product)
            & (df["entry_point"] == origin)
        ].copy()

        if tmp.empty:
            return result

        if len(tmp) == 1:
            row = tmp.iloc[0]
            result["purchase_origin"] = normalize_text(row["origin"])
            result["purchase_quantity_kg_ref"] = safe_float(row["purchase_quantity_kg"])
            result["purchase_unit_cost_usd_per_kg"] = safe_float(row["purchase_unit_cost_usd_per_kg"])
            result["purchase_cost_usd"] = quantity_kg * result["purchase_unit_cost_usd_per_kg"]
            return result

        total_qty = safe_float(tmp["purchase_quantity_kg"].sum())
        total_cost = safe_float(tmp["purchase_total_cost_usd"].sum())
        weighted_unit_cost = (total_cost / total_qty) if total_qty > 0 else 0.0

        distinct_countries = sorted(set(tmp["origin"].dropna().astype(str).tolist()))
        if len(distinct_countries) == 1:
            purchase_origin = distinct_countries[0]
        elif len(distinct_countries) > 1:
            purchase_origin = "Mixed"
        else:
            purchase_origin = "Unknown"

        result["purchase_origin"] = purchase_origin
        result["purchase_quantity_kg_ref"] = total_qty
        result["purchase_unit_cost_usd_per_kg"] = weighted_unit_cost
        result["purchase_cost_usd"] = quantity_kg * weighted_unit_cost
        return result

    if flow_type == "Scl_to_DC":
        df = purchase_aggs["domestic_by_origin_product_month"]
        if df.empty:
            result["purchase_origin"] = "Chile"
            return result

        tmp = df[
            (df["month"] == month)
            & (df["product"] == product)
            & (df["origin"] == origin)
        ].copy()

        result["purchase_origin"] = "Chile"

        if tmp.empty:
            return result

        row = tmp.iloc[0]
        result["purchase_quantity_kg_ref"] = safe_float(row["purchase_quantity_kg"])
        result["purchase_unit_cost_usd_per_kg"] = safe_float(row["purchase_unit_cost_usd_per_kg"])
        result["purchase_cost_usd"] = quantity_kg * result["purchase_unit_cost_usd_per_kg"]
        return result

    if flow_type == "WM_to_DC":
        df = purchase_aggs["wm_by_origin_product_month"]
        if df.empty:
            result["purchase_origin"] = "Chile"
            return result

        tmp = df[
            (df["month"] == month)
            & (df["product"] == product)
            & (df["origin"] == origin)
        ].copy()

        result["purchase_origin"] = "Chile"

        if tmp.empty:
            return result

        row = tmp.iloc[0]
        result["purchase_quantity_kg_ref"] = safe_float(row["purchase_quantity_kg"])
        result["purchase_unit_cost_usd_per_kg"] = safe_float(row["purchase_unit_cost_usd_per_kg"])
        result["purchase_cost_usd"] = quantity_kg * result["purchase_unit_cost_usd_per_kg"]
        return result

    return result


# ============================================================
# SUPPLIER -> DC DATA BUILD
# ============================================================
def build_supply_to_dc_connections(
    model_name: str,
    months: List[str],
    df_flows: pd.DataFrame,
    df_purchases: pd.DataFrame,
    lookups: Dict[str, Dict[str, Dict[str, Any]]],
) -> pd.DataFrame:
    if df_flows.empty:
        return pd.DataFrame()

    flows = df_flows.copy()

    if "flow_type" in flows.columns:
        flows["flow_type"] = flows["flow_type"].apply(normalize_flow_type)

    flows = flows[flows["flow_type"].isin(VALID_SUPPLY_TO_DC_FLOW_TYPES)].copy()

    if flows.empty:
        return pd.DataFrame()

    flows["quantity_kg"] = flows["quantity_kg"].apply(safe_float)
    flows["distance_km"] = flows["distance_km"].apply(safe_float)

    if "month" in flows.columns:
        flows["month"] = flows["month"].apply(normalize_text)

    if "origin" in flows.columns:
        flows["origin"] = flows["origin"].apply(normalize_text)

    if "destination" in flows.columns:
        flows["destination"] = flows["destination"].apply(normalize_text)

    if "product" in flows.columns:
        flows["product"] = flows["product"].apply(normalize_text)
    else:
        flows["product"] = ""

    grouped_flows = (
        flows.groupby(["month", "flow_type", "product", "origin", "destination"], as_index=False)
        .agg(
            quantity_kg=("quantity_kg", "sum"),
            distance_km=("distance_km", "mean"),
        )
    )

    purchase_aggs = build_purchase_aggregates(df_purchases)

    rows: List[Dict[str, Any]] = []

    for _, row in grouped_flows.iterrows():
        month = normalize_text(row["month"])
        flow_type = normalize_flow_type(row["flow_type"])
        product = normalize_text(row["product"])
        origin = normalize_text(row["origin"])
        destination = normalize_text(row["destination"])
        quantity_kg = safe_float(row["quantity_kg"])
        distance_km = safe_float(row["distance_km"])

        origin_info = get_origin_info(origin, lookups)
        destination_info = get_destination_info(destination, lookups)

        if origin_info is None:
            log(f"[WARNING] {model_name} | {month}: origen no encontrado en lookups: {origin}")
            continue

        if destination_info is None:
            log(f"[WARNING] {model_name} | {month}: destino no encontrado en lookups: {destination}")
            continue

        purchase_info = resolve_purchase_info_for_connection(
            flow_type=flow_type,
            month=month,
            product=product,
            origin=origin,
            quantity_kg=quantity_kg,
            purchase_aggs=purchase_aggs,
        )

        purchase_origin = normalize_text(purchase_info["purchase_origin"])
        origin_country = infer_origin_country_from_row(
            flow_type=flow_type,
            origin_name=origin,
            purchase_origin=purchase_origin,
        )

        transport_unit_cost_usd_per_kg_km = get_transport_rate(flow_type, product)
        transport_cost_usd = quantity_kg * distance_km * transport_unit_cost_usd_per_kg_km
        purchase_cost_usd = safe_float(purchase_info["purchase_cost_usd"])
        total_landed_cost_usd = purchase_cost_usd + transport_cost_usd
        unit_landed_cost_usd_per_kg = (
            total_landed_cost_usd / quantity_kg if quantity_kg > 0 else 0.0
        )

        rows.append({
            "model": model_name,
            "month": month,
            "flow_type": flow_type,
            "product": product,

            "origin": origin,
            "origin_category": origin_info["node_category"],
            "origin_type": origin_info["type"],
            "origin_lat": origin_info["lat"],
            "origin_lon": origin_info["lon"],

            "destination": destination,
            "destination_category": destination_info["node_category"],
            "destination_type": destination_info["type"],
            "destination_lat": destination_info["lat"],
            "destination_lon": destination_info["lon"],

            "quantity_kg": quantity_kg,
            "distance_km": distance_km,

            "supplier_type": (
                "Import"
                if flow_type == "E_to_DC"
                else "Domestic Supplier" if flow_type == "Scl_to_DC"
                else "Wholesale Market"
            ),
            "origin_country": origin_country,
            "purchase_origin": purchase_origin,

            "purchase_quantity_kg_ref": purchase_info["purchase_quantity_kg_ref"],
            "purchase_unit_cost_usd_per_kg": safe_float(purchase_info["purchase_unit_cost_usd_per_kg"]),
            "purchase_cost_usd": purchase_cost_usd,

            "transport_unit_cost_usd_per_kg_km": transport_unit_cost_usd_per_kg_km,
            "transport_cost_usd": transport_cost_usd,

            "total_landed_cost_usd": total_landed_cost_usd,
            "unit_landed_cost_usd_per_kg": unit_landed_cost_usd_per_kg,
        })

    out = pd.DataFrame(rows)

    if out.empty:
        return out

    month_totals = (
        out.groupby(["model", "month"], as_index=False)
        .agg(month_total_quantity_kg=("quantity_kg", "sum"))
    )

    out = out.merge(month_totals, on=["model", "month"], how="left")

    out["share_of_month_inbound_pct"] = out.apply(
        lambda r: (100.0 * safe_float(r["quantity_kg"]) / safe_float(r["month_total_quantity_kg"]))
        if safe_float(r["month_total_quantity_kg"]) > 0
        else 0.0,
        axis=1,
    )

    out = out.sort_values(
        by=["model", "month", "flow_type", "product", "origin", "destination"],
        key=lambda col: col.map(month_sort_key) if col.name == "month" else col,
    ).reset_index(drop=True)

    return out


def build_suppliers_month(
    model_name: str,
    months: List[str],
    supply_to_dc_connections: pd.DataFrame,
) -> pd.DataFrame:
    if supply_to_dc_connections.empty:
        return pd.DataFrame()

    df = supply_to_dc_connections.copy()

    out = (
        df.groupby(
            ["model", "month", "origin", "origin_category", "origin_type", "origin_lat", "origin_lon"],
            as_index=False
        )
        .agg(
            total_flow_kg=("quantity_kg", "sum"),
            purchase_cost_usd=("purchase_cost_usd", "sum"),
            transport_cost_usd=("transport_cost_usd", "sum"),
            total_landed_cost_usd=("total_landed_cost_usd", "sum"),
            n_destinations=("destination", "nunique"),
        )
        .rename(columns={
            "origin": "supplier_name",
            "origin_category": "supplier_category",
            "origin_type": "supplier_type",
            "origin_lat": "supplier_lat",
            "origin_lon": "supplier_lon",
        })
    )

    return out.sort_values(
        by=["model", "month", "supplier_category", "supplier_name"],
        key=lambda col: col.map(month_sort_key) if col.name == "month" else col,
    ).reset_index(drop=True)


def build_dcs_month(
    model_name: str,
    months: List[str],
    supply_to_dc_connections: pd.DataFrame,
) -> pd.DataFrame:
    if supply_to_dc_connections.empty:
        return pd.DataFrame()

    df = supply_to_dc_connections.copy()

    out = (
        df.groupby(
            [
                "model",
                "month",
                "destination",
                "destination_category",
                "destination_type",
                "destination_lat",
                "destination_lon",
            ],
            as_index=False
        )
        .agg(
            total_inbound_kg=("quantity_kg", "sum"),
            total_purchase_cost_usd=("purchase_cost_usd", "sum"),
            total_transport_cost_usd=("transport_cost_usd", "sum"),
            total_landed_cost_usd=("total_landed_cost_usd", "sum"),
            n_origins=("origin", "nunique"),
        )
        .rename(columns={
            "destination": "dc_name",
            "destination_category": "dc_category",
            "destination_type": "dc_type",
            "destination_lat": "dc_lat",
            "destination_lon": "dc_lon",
        })
    )

    return out.sort_values(
        by=["model", "month", "dc_name"],
        key=lambda col: col.map(month_sort_key) if col.name == "month" else col,
    ).reset_index(drop=True)


def build_supplier_to_dc_summary(
    model_name: str,
    months: List[str],
    supply_to_dc_connections: pd.DataFrame,
) -> pd.DataFrame:
    if supply_to_dc_connections.empty:
        return pd.DataFrame()

    df = supply_to_dc_connections.copy()

    summary_df = (
        df.groupby(
            [
                "model",
                "month",
                "flow_type",
                "supplier_type",
                "origin_country",
                "origin",
                "destination",
                "product",
                "origin_category",
                "destination_type",
                "origin_lat",
                "origin_lon",
                "destination_lat",
                "destination_lon",
            ],
            as_index=False
        )
        .agg(
            quantity_kg=("quantity_kg", "sum"),
            distance_km=("distance_km", "mean"),
            purchase_unit_cost_usd_per_kg=("purchase_unit_cost_usd_per_kg", "mean"),
            purchase_cost_usd=("purchase_cost_usd", "sum"),
            transport_unit_cost_usd_per_kg_km=("transport_unit_cost_usd_per_kg_km", "mean"),
            transport_cost_usd=("transport_cost_usd", "sum"),
            total_landed_cost_usd=("total_landed_cost_usd", "sum"),
            month_total_quantity_kg=("month_total_quantity_kg", "max"),
        )
    )

    summary_df["unit_landed_cost_usd_per_kg"] = summary_df.apply(
        lambda r: safe_float(r["total_landed_cost_usd"]) / safe_float(r["quantity_kg"])
        if safe_float(r["quantity_kg"]) > 0
        else 0.0,
        axis=1,
    )

    summary_df["share_of_month_inbound_pct"] = summary_df.apply(
        lambda r: (100.0 * safe_float(r["quantity_kg"]) / safe_float(r["month_total_quantity_kg"]))
        if safe_float(r["month_total_quantity_kg"]) > 0
        else 0.0,
        axis=1,
    )

    summary_df = summary_df.sort_values(
        by=["model", "month", "flow_type", "product", "origin", "destination"],
        key=lambda col: col.map(month_sort_key) if col.name == "month" else col,
    ).reset_index(drop=True)

    return summary_df


# ============================================================
# MODEL BUILD
# ============================================================
def build_model_plot_data(
    model_context: Dict[str, Any],
    lookups: Dict[str, Dict[str, Dict[str, Any]]],
) -> Dict[str, Any]:
    model_name = model_context["model_name"]
    months = list(model_context.get("months", []))
    summary = model_context.get("summary", {})

    log(f"[PROCESS] {model_name}")

    raw_csvs = load_model_csvs(model_context)

    months = ordered_unique_months(
        raw_csvs["flows"],
        raw_csvs["purchases"],
        raw_csvs["deliveries"],
        raw_csvs["mdc_assignment"],
    ) or months

    df_supply_to_dc_connections = build_supply_to_dc_connections(
        model_name=model_name,
        months=months,
        df_flows=raw_csvs["flows"],
        df_purchases=raw_csvs["purchases"],
        lookups=lookups,
    )

    df_suppliers_month = build_suppliers_month(
        model_name=model_name,
        months=months,
        supply_to_dc_connections=df_supply_to_dc_connections,
    )

    df_dcs_month = build_dcs_month(
        model_name=model_name,
        months=months,
        supply_to_dc_connections=df_supply_to_dc_connections,
    )

    df_supplier_to_dc_summary = build_supplier_to_dc_summary(
        model_name=model_name,
        months=months,
        supply_to_dc_connections=df_supply_to_dc_connections,
    )

    log(
        f"    supplier_to_dc connections={len(df_supply_to_dc_connections)} | "
        f"suppliers_month={len(df_suppliers_month)} | "
        f"dcs_month={len(df_dcs_month)} | "
        f"summary={len(df_supplier_to_dc_summary)}"
    )

    return {
        "summary": summary,
        "months": months,
        "raw": raw_csvs,
        "supplier_to_dc": {
            "connections": df_supply_to_dc_connections,
            "suppliers_month": df_suppliers_month,
            "dcs_month": df_dcs_month,
            "summary": df_supplier_to_dc_summary,
        },
    }


# ============================================================
# GLOBAL BUILD
# ============================================================
def build_plot_data(
    base_results_dir: Path = BASE_RESULTS_DIR,
    model_db_path: Path = MODEL_DB,
) -> Dict[str, Any]:
    section("BUILD PLOT DATA")

    model_contexts = load_model_contexts(base_results_dir)
    reference_tables = load_reference_tables(model_db_path)
    lookups = build_reference_lookups(reference_tables)

    plot_data: Dict[str, Any] = {
        "global": {
            "base_results_dir": str(base_results_dir.resolve()),
            "model_db_path": str(model_db_path.resolve()),
            "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "lookups": lookups,
            "transport_costs": {
                "C_TR_LONG": C_TR_LONG,
                "C_TR_INT": C_TR_INT,
                "C_LM": C_LM,
                "C_MOV": C_MOV,
            },
        },
        "models": {},
    }

    all_months = set()

    for model_context in model_contexts:
        model_name = model_context["model_name"]
        model_plot_data = build_model_plot_data(model_context, lookups)
        plot_data["models"][model_name] = model_plot_data
        all_months.update(model_plot_data["months"])

    plot_data["global"]["models_found"] = sorted(list(plot_data["models"].keys()))
    plot_data["global"]["all_months_found"] = sorted(list(all_months), key=month_sort_key)

    return plot_data


# ============================================================
# PUBLIC ENTRYPOINT
# ============================================================
def run_build_plot_data(
    base_results_dir: Path = BASE_RESULTS_DIR,
    model_db_path: Path = MODEL_DB,
) -> Dict[str, Any]:
    plot_data = build_plot_data(
        base_results_dir=base_results_dir,
        model_db_path=model_db_path,
    )

    section("BUILD PLOT DATA COMPLETE")
    log(
        f"Models found: {len(plot_data['global']['models_found'])} | "
        f"Months found: {len(plot_data['global']['all_months_found'])}"
    )

    for model_name, model_data in plot_data["models"].items():
        conn_count = len(model_data["supplier_to_dc"]["connections"])
        sum_count = len(model_data["supplier_to_dc"]["summary"])
        log(
            f"    {model_name:<15} | "
            f"supplier_to_dc connections = {conn_count:<5} | "
            f"summary = {sum_count}"
        )

    return plot_data
# ============================================================
# OPTIONAL DEBUG EXPORT
# ============================================================
def export_debug_outputs(plot_data: Dict[str, Any]) -> None:
    section("EXPORT DEBUG OUTPUTS")

    for model_name, model_data in plot_data["models"].items():
        model_dir = Path(plot_data["global"]["base_results_dir"]) / model_name
        graph_dir = model_dir / GRAPH_DIR_NAME
        graph_dir.mkdir(parents=True, exist_ok=True)

        supplier_data = model_data["supplier_to_dc"]

        connections_df = supplier_data["connections"]
        suppliers_df = supplier_data["suppliers_month"]
        dcs_df = supplier_data["dcs_month"]
        summary_df = supplier_data["summary"]

        if not connections_df.empty:
            path = graph_dir / "supplier_to_dc_connections_debug.csv"
            connections_df.to_csv(path, index=False)
            log(f"[OK] {path}")

        if not suppliers_df.empty:
            path = graph_dir / "suppliers_month_debug.csv"
            suppliers_df.to_csv(path, index=False)
            log(f"[OK] {path}")

        if not dcs_df.empty:
            path = graph_dir / "dcs_month_debug.csv"
            dcs_df.to_csv(path, index=False)
            log(f"[OK] {path}")

        if not summary_df.empty:
            path = graph_dir / "supplier_to_dc_summary_debug.csv"
            summary_df.to_csv(path, index=False)
            log(f"[OK] {path}")


# ============================================================
# MAIN
# ============================================================
def main() -> None:
    section("RUN 01 BUILD PLOT DATA")

    plot_data = run_build_plot_data(
        base_results_dir=BASE_RESULTS_DIR,
        model_db_path=MODEL_DB,
    )

    # OPCIONAL: exportar CSVs para debug
    export_debug_outputs(plot_data)

    section("DONE")


if __name__ == "__main__":
    main()