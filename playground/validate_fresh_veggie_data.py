import sqlite3
from pathlib import Path
from typing import Dict, List, Tuple, Any

import pandas as pd


# ============================================================
# CONFIG
# ============================================================
MODEL_DB = Path("Sets/model.db")
PARAM_DB = Path("Sets/parameters.db")
TRANSPORT_DB = Path("Sets/transport_matrices.db")


# ============================================================
# BASIC HELPERS
# ============================================================
def log(msg: str) -> None:
    print(msg)


def section(title: str) -> None:
    print("\n" + "=" * 90)
    print(title)
    print("=" * 90)


def norm(x: Any) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip().lower()


def safe_float(x, default=None):
    try:
        if pd.isna(x):
            return default
        return float(x)
    except Exception:
        return default


def fetch_table(db_path: Path, table_name: str) -> pd.DataFrame:
    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query(f'SELECT * FROM "{table_name}"', conn)
    finally:
        conn.close()

    for col in df.columns:
        if pd.api.types.is_object_dtype(df[col]):
            df[col] = df[col].astype(str).str.strip()

    return df


def print_df_preview(df: pd.DataFrame, title: str, max_rows: int = 20) -> None:
    if df.empty:
        log(f"{title}: <empty>")
    else:
        log(title)
        print(df.head(max_rows).to_string(index=False))


# ============================================================
# VALIDATION REPORT
# ============================================================
class ValidationReport:
    def __init__(self):
        self.errors: List[str] = []
        self.warnings: List[str] = []
        self.infos: List[str] = []

    def error(self, msg: str) -> None:
        self.errors.append(msg)

    def warning(self, msg: str) -> None:
        self.warnings.append(msg)

    def info(self, msg: str) -> None:
        self.infos.append(msg)

    def summary(self) -> None:
        section("VALIDATION SUMMARY")
        log(f"INFO     : {len(self.infos)}")
        log(f"WARNINGS : {len(self.warnings)}")
        log(f"ERRORS   : {len(self.errors)}")

        if self.infos:
            print("\n[INFO]")
            for msg in self.infos:
                print(f"- {msg}")

        if self.warnings:
            print("\n[WARNINGS]")
            for msg in self.warnings:
                print(f"- {msg}")

        if self.errors:
            print("\n[ERRORS]")
            for msg in self.errors:
                print(f"- {msg}")


# ============================================================
# DOMAIN NORMALIZATION
# ============================================================
def normalize_product_name(x: str) -> str:
    x0 = norm(x)

    aliases = {
        "roma tomato": "roma tomatoes",
        "roma tomatoes": "roma tomatoes",
        "tomato roma": "roma tomatoes",

        "cauliflower": "cauliflower",

        "broccoli": "broccoli",
        "brocolli": "broccoli",

        "asparagus": "asparagus",

        "green bell pepper": "green bell pepper",
        "green bell peppers": "green bell pepper",
        "green pepper": "green bell pepper",
        "bell pepper": "green bell pepper",
        "pepper": "green bell pepper",
    }

    return aliases.get(x0, x0)


def normalize_origin_name(x: str) -> str:
    x0 = norm(x)

    aliases = {
        "us": "usa",
        "usa": "usa",
        "united states": "usa",
        "united states of america": "usa",

        "mexico": "mexico",
        "peru": "peru",
        "ecuador": "ecuador",
        "bolivia": "bolivia",
        "argentina": "argentina",
        "spain": "spain",

        "chile-scl": "chile-scl",
        "chile-vap": "chile-vap",
        "chile-maul": "chile-maul",
        "chile-coq": "chile-coq",

        "la vega central": "la vega central",
        "lo valledor": "lo valledor",
    }

    return aliases.get(x0, x0)


def normalize_entry_type(x: str) -> str:
    x0 = norm(x)

    aliases = {
        "airport": "airport",
        "air": "airport",

        "port": "port",
        "sea": "port",
        "seaport": "port",
        "harbor": "port",

        "land customs": "land customs",
        "land": "land customs",
        "customs": "land customs",
        "border": "land customs",
    }

    return aliases.get(x0, x0)


# ============================================================
# TABLE PRESENCE / COLUMNS
# ============================================================
def validate_required_tables(report: ValidationReport) -> Dict[str, pd.DataFrame]:
    section("LOADING REQUIRED TABLES")

    required = {
        "E": (MODEL_DB, "E"),
        "F": (MODEL_DB, "F"),
        "K": (MODEL_DB, "K"),
        "S_imp": (MODEL_DB, "S_imp"),
        "S_cl": (MODEL_DB, "S_cl"),
        "WM": (MODEL_DB, "WM"),
        "M": (PARAM_DB, "M"),
        "P_k": (PARAM_DB, "P_k"),
        "C_pur_imp": (PARAM_DB, "C_pur_imp"),
        "C_pur_cl": (PARAM_DB, "C_pur_cl"),
        "C_pur_wm": (PARAM_DB, "C_pur_wm"),
        "a_ksm": (PARAM_DB, "a_ksm"),
    }

    data = {}

    for key, (db_path, table_name) in required.items():
        try:
            df = fetch_table(db_path, table_name)
            data[key] = df
            report.info(f"Loaded table {key} from {db_path}")
            log(f"Loaded {key:<10} rows={len(df):>5} cols={len(df.columns):>3}")
        except Exception as e:
            report.error(f"Could not load table {table_name} from {db_path}: {e}")

    return data


def validate_required_columns(data: Dict[str, pd.DataFrame], report: ValidationReport) -> None:
    section("VALIDATING REQUIRED COLUMNS")

    required_columns = {
        "E": ["international entry point", "type"],
        "F": ["facility_name", "type"],
        "K": ["product"],
        "S_imp": ["origin"],
        "S_cl": ["origin"],
        "WM": ["wholesale_market"],
        "M": ["month_num", "month_name"],
        "P_k": ["product", "price_usd_per_kg"],
        "C_pur_imp": [
            "origin",
            "product",
            "purchase_cost_usd_per_kg_sea",
            "purchase_cost_usd_per_kg_air",
            "purchase_cost_usd_per_kg_land",
        ],
        "C_pur_cl": ["origin", "product", "cost_usd_per_kg"],
        "C_pur_wm": ["product", "cost_usd_per_kg"],
        "a_ksm": ["origin", "product"],
    }

    for table_name, cols in required_columns.items():
        if table_name not in data:
            continue

        df = data[table_name]
        missing = [c for c in cols if c not in df.columns]
        if missing:
            report.error(f"Table {table_name} is missing columns: {missing}")
        else:
            report.info(f"Table {table_name} has required columns")


# ============================================================
# MASTER SET EXTRACTION
# ============================================================
def build_master_sets(data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    K_raw = sorted(data["K"]["product"].dropna().astype(str).str.strip().tolist())
    S_imp_raw = sorted(data["S_imp"]["origin"].dropna().astype(str).str.strip().tolist())
    S_cl_raw = sorted(data["S_cl"]["origin"].dropna().astype(str).str.strip().tolist())
    E_raw = sorted(data["E"]["international entry point"].dropna().astype(str).str.strip().tolist())
    WM_raw = sorted(data["WM"]["wholesale_market"].dropna().astype(str).str.strip().tolist())

    e_type = {
        str(row["international entry point"]).strip(): str(row["type"]).strip()
        for _, row in data["E"].iterrows()
    }

    months = (
        data["M"]
        .sort_values("month_num")["month_name"]
        .dropna()
        .astype(str)
        .str.strip()
        .tolist()
    )

    return {
        "K_raw": K_raw,
        "S_imp_raw": S_imp_raw,
        "S_cl_raw": S_cl_raw,
        "E_raw": E_raw,
        "WM_raw": WM_raw,
        "M_raw": months,
        "e_type": e_type,

        "K_norm": sorted({normalize_product_name(x) for x in K_raw}),
        "S_imp_norm": sorted({normalize_origin_name(x) for x in S_imp_raw}),
        "S_cl_norm": sorted({normalize_origin_name(x) for x in S_cl_raw}),
        "WM_norm": sorted({normalize_origin_name(x) for x in WM_raw}),
    }


# ============================================================
# PRODUCT / ORIGIN CONSISTENCY
# ============================================================
def validate_product_and_origin_consistency(data: Dict[str, pd.DataFrame], master: Dict[str, Any], report: ValidationReport) -> None:
    section("VALIDATING PRODUCT / ORIGIN CONSISTENCY")

    # K vs C_pur_imp
    cpurimp_products_raw = sorted(set(data["C_pur_imp"]["product"].dropna().astype(str).str.strip()))
    cpurimp_products_norm = sorted({normalize_product_name(x) for x in cpurimp_products_raw})

    k_raw = master["K_raw"]
    k_norm = master["K_norm"]

    missing_in_cpurimp_norm = sorted(set(k_norm) - set(cpurimp_products_norm))
    extra_in_cpurimp_norm = sorted(set(cpurimp_products_norm) - set(k_norm))

    if missing_in_cpurimp_norm:
        report.error(f"Products in K missing from C_pur_imp after normalization: {missing_in_cpurimp_norm}")
    if extra_in_cpurimp_norm:
        report.warning(f"Products in C_pur_imp not present in K after normalization: {extra_in_cpurimp_norm}")

    cpurimp_origins_raw = sorted(set(data["C_pur_imp"]["origin"].dropna().astype(str).str.strip()))
    cpurimp_origins_norm = sorted({normalize_origin_name(x) for x in cpurimp_origins_raw})

    simp_raw = master["S_imp_raw"]
    simp_norm = master["S_imp_norm"]

    missing_origins_in_cpurimp_norm = sorted(set(simp_norm) - set(cpurimp_origins_norm))
    extra_origins_in_cpurimp_norm = sorted(set(cpurimp_origins_norm) - set(simp_norm))

    if missing_origins_in_cpurimp_norm:
        report.error(f"Origins in S_imp missing from C_pur_imp after normalization: {missing_origins_in_cpurimp_norm}")
    if extra_origins_in_cpurimp_norm:
        report.warning(f"Origins in C_pur_imp not present in S_imp after normalization: {extra_origins_in_cpurimp_norm}")

    # Raw mismatches are useful too
    missing_in_cpurimp_raw = sorted(set(k_raw) - set(cpurimp_products_raw))
    if missing_in_cpurimp_raw:
        report.warning(f"Raw product names from K not found exactly in C_pur_imp: {missing_in_cpurimp_raw}")

    missing_origins_in_cpurimp_raw = sorted(set(simp_raw) - set(cpurimp_origins_raw))
    if missing_origins_in_cpurimp_raw:
        report.warning(f"Raw origins from S_imp not found exactly in C_pur_imp: {missing_origins_in_cpurimp_raw}")

    # Domestic
    cpurcl_products_norm = sorted({normalize_product_name(x) for x in data["C_pur_cl"]["product"].dropna().astype(str).str.strip()})
    cpurcl_origins_norm = sorted({normalize_origin_name(x) for x in data["C_pur_cl"]["origin"].dropna().astype(str).str.strip()})

    missing_in_cpurcl_products = sorted(set(k_norm) - set(cpurcl_products_norm))
    missing_in_cpurcl_origins = sorted(set(master["S_cl_norm"]) - set(cpurcl_origins_norm))

    if missing_in_cpurcl_products:
        report.error(f"Products in K missing from C_pur_cl after normalization: {missing_in_cpurcl_products}")
    if missing_in_cpurcl_origins:
        report.error(f"Origins in S_cl missing from C_pur_cl after normalization: {missing_in_cpurcl_origins}")

    # WM
    cpurwm_products_norm = sorted({normalize_product_name(x) for x in data["C_pur_wm"]["product"].dropna().astype(str).str.strip()})
    missing_in_cpurwm_products = sorted(set(k_norm) - set(cpurwm_products_norm))
    if missing_in_cpurwm_products:
        report.error(f"Products in K missing from C_pur_wm after normalization: {missing_in_cpurwm_products}")


# ============================================================
# ENTRY TYPES
# ============================================================
def validate_entry_point_types(master: Dict[str, Any], report: ValidationReport) -> None:
    section("VALIDATING ENTRY POINT TYPES")

    bad = []
    for e, t in master["e_type"].items():
        t_norm = normalize_entry_type(t)
        if t_norm not in {"airport", "port", "land customs"}:
            bad.append((e, t))

    if bad:
        report.error(f"Unknown entry point types found: {bad}")
    else:
        report.info("All entry point types are valid after normalization")


# ============================================================
# IMPORT COST MATRIX COVERAGE
# ============================================================
def validate_import_cost_matrix(data: Dict[str, pd.DataFrame], master: Dict[str, Any], report: ValidationReport) -> pd.DataFrame:
    section("VALIDATING IMPORT COST MATRIX")

    df = data["C_pur_imp"].copy()

    df["product_norm"] = df["product"].apply(normalize_product_name)
    df["origin_norm"] = df["origin"].apply(normalize_origin_name)

    records = []
    missing_rows = []
    duplicate_rows = []

    grouped = df.groupby(["product_norm", "origin_norm"], dropna=False).size().reset_index(name="count")
    dup_df = grouped[grouped["count"] > 1].copy()

    if not dup_df.empty:
        for _, row in dup_df.iterrows():
            duplicate_rows.append((row["product_norm"], row["origin_norm"], int(row["count"])))
        report.warning(f"Duplicate rows in C_pur_imp by normalized (product, origin): {duplicate_rows[:20]}")

    raw_lookup = {}
    for _, row in df.iterrows():
        raw_lookup[(row["product_norm"], row["origin_norm"])] = {
            "sea": safe_float(row["purchase_cost_usd_per_kg_sea"], None),
            "air": safe_float(row["purchase_cost_usd_per_kg_air"], None),
            "land": safe_float(row["purchase_cost_usd_per_kg_land"], None),
        }

    for k in master["K_raw"]:
        for s in master["S_imp_raw"]:
            k_norm = normalize_product_name(k)
            s_norm = normalize_origin_name(s)

            if (k_norm, s_norm) not in raw_lookup:
                missing_rows.append((k, s, "missing product-origin row"))
                continue

            for e in master["E_raw"]:
                e_type_raw = master["e_type"].get(e, "")
                e_type_norm = normalize_entry_type(e_type_raw)

                mode_map = {
                    "port": "sea",
                    "airport": "air",
                    "land customs": "land",
                }

                if e_type_norm not in mode_map:
                    missing_rows.append((k, s, f"{e} invalid entry type {e_type_raw}"))
                    continue

                mode = mode_map[e_type_norm]
                cost = raw_lookup[(k_norm, s_norm)].get(mode, None)

                records.append({
                    "product_model": k,
                    "origin_model": s,
                    "entry_point": e,
                    "entry_type_raw": e_type_raw,
                    "entry_type_norm": e_type_norm,
                    "mode_column_used": mode,
                    "cost_value": cost,
                })

                if cost is None:
                    missing_rows.append((k, s, f"{e} missing cost"))
                elif cost < 0:
                    missing_rows.append((k, s, f"{e} negative cost {cost}"))

    audit_df = pd.DataFrame(records)

    if missing_rows:
        report.error(f"C_pur_imp has missing/invalid combinations. First cases: {missing_rows[:25]}")
    else:
        report.info("C_pur_imp covers all (product, origin, entry point) combinations")

    if not audit_df.empty:
        zero_df = audit_df[audit_df["cost_value"].fillna(-1) == 0]
        huge_df = audit_df[audit_df["cost_value"].fillna(0) >= 1e9]

        if not zero_df.empty:
            report.warning(f"There are import costs equal to 0. First cases: {zero_df.head(20).to_dict('records')}")

        if not huge_df.empty:
            report.warning(f"There are import costs >= 1e9 acting as prohibitive penalties. First cases: {huge_df.head(20).to_dict('records')}")

        print_df_preview(audit_df, "Preview of resolved import cost audit:")

    return audit_df


# ============================================================
# DOMESTIC / WM COST MATRIX COVERAGE
# ============================================================
def validate_domestic_and_wm_costs(data: Dict[str, pd.DataFrame], master: Dict[str, Any], report: ValidationReport) -> None:
    section("VALIDATING DOMESTIC AND WM COSTS")

    # Domestic
    df_cl = data["C_pur_cl"].copy()
    df_cl["product_norm"] = df_cl["product"].apply(normalize_product_name)
    df_cl["origin_norm"] = df_cl["origin"].apply(normalize_origin_name)

    cl_lookup = {
        (row["product_norm"], row["origin_norm"]): safe_float(row["cost_usd_per_kg"], None)
        for _, row in df_cl.iterrows()
    }

    missing_cl = []
    for k in master["K_raw"]:
        for s in master["S_cl_raw"]:
            key = (normalize_product_name(k), normalize_origin_name(s))
            if key not in cl_lookup:
                missing_cl.append((k, s, "missing row"))
            else:
                c = cl_lookup[key]
                if c is None:
                    missing_cl.append((k, s, "null cost"))
                elif c < 0:
                    missing_cl.append((k, s, f"negative cost {c}"))

    if missing_cl:
        report.error(f"C_pur_cl has missing/invalid combinations. First cases: {missing_cl[:25]}")
    else:
        report.info("C_pur_cl covers all (product, origin) combinations")

    # WM
    df_wm = data["C_pur_wm"].copy()
    df_wm["product_norm"] = df_wm["product"].apply(normalize_product_name)

    wm_lookup = {
        row["product_norm"]: safe_float(row["cost_usd_per_kg"], None)
        for _, row in df_wm.iterrows()
    }

    missing_wm = []
    for k in master["K_raw"]:
        key = normalize_product_name(k)
        if key not in wm_lookup:
            missing_wm.append((k, "missing row"))
        else:
            c = wm_lookup[key]
            if c is None:
                missing_wm.append((k, "null cost"))
            elif c < 0:
                missing_wm.append((k, f"negative cost {c}"))

    if missing_wm:
        report.error(f"C_pur_wm has missing/invalid products. First cases: {missing_wm[:25]}")
    else:
        report.info("C_pur_wm covers all products")


# ============================================================
# AVAILABILITY TABLE
# ============================================================
def validate_availability_table(data: Dict[str, pd.DataFrame], master: Dict[str, Any], report: ValidationReport) -> None:
    section("VALIDATING AVAILABILITY TABLE a_ksm")

    df = data["a_ksm"].copy()

    month_cols_required = [f"available_{m}" for m in master["M_raw"]]
    missing_cols = [c for c in month_cols_required if c not in df.columns]
    if missing_cols:
        report.error(f"a_ksm is missing month columns: {missing_cols}")
        return

    df["product_norm"] = df["product"].apply(normalize_product_name)
    df["origin_norm"] = df["origin"].apply(normalize_origin_name)

    avail_keys = set(zip(df["product_norm"], df["origin_norm"]))

    all_origins = master["S_imp_raw"] + master["S_cl_raw"]
    missing_keys = []

    for k in master["K_raw"]:
        for s in all_origins:
            key = (normalize_product_name(k), normalize_origin_name(s))
            if key not in avail_keys:
                missing_keys.append((k, s))

    if missing_keys:
        report.warning(f"a_ksm is missing product-origin rows. First cases: {missing_keys[:25]}")
    else:
        report.info("a_ksm covers all product-origin combinations")

    bad_values = []
    for _, row in df.iterrows():
        for col in month_cols_required:
            val = safe_float(row[col], None)
            if val not in (0, 1, 0.0, 1.0):
                bad_values.append((row["product"], row["origin"], col, row[col]))

    if bad_values:
        report.warning(f"a_ksm has values different from 0/1. First cases: {bad_values[:25]}")
    else:
        report.info("a_ksm month values are binary")


# ============================================================
# SELLING PRICE COVERAGE
# ============================================================
def validate_selling_prices(data: Dict[str, pd.DataFrame], master: Dict[str, Any], report: ValidationReport) -> None:
    section("VALIDATING SELLING PRICES P_k")

    df = data["P_k"].copy()
    df["product_norm"] = df["product"].apply(normalize_product_name)

    price_lookup = {
        row["product_norm"]: safe_float(row["price_usd_per_kg"], None)
        for _, row in df.iterrows()
    }

    missing = []
    bad = []
    for k in master["K_raw"]:
        kn = normalize_product_name(k)
        if kn not in price_lookup:
            missing.append(k)
        else:
            v = price_lookup[kn]
            if v is None or v <= 0:
                bad.append((k, v))

    if missing:
        report.error(f"P_k missing products: {missing}")
    if bad:
        report.error(f"P_k has non-positive/invalid prices: {bad}")
    if not missing and not bad:
        report.info("P_k covers all products with positive prices")


# ============================================================
# OPTIONAL JOIN DEMO
# ============================================================
def print_exact_mismatch_demo(data: Dict[str, pd.DataFrame], master: Dict[str, Any]) -> None:
    section("EXACT MATCH VS NORMALIZED MATCH DEMO")

    df = data["C_pur_imp"].copy()

    exact_keys = set(zip(
        df["product"].astype(str).str.strip(),
        df["origin"].astype(str).str.strip()
    ))

    norm_keys = set(zip(
        df["product"].apply(normalize_product_name),
        df["origin"].apply(normalize_origin_name)
    ))

    rows = []
    for k in master["K_raw"]:
        for s in master["S_imp_raw"]:
            rows.append({
                "product_model": k,
                "origin_model": s,
                "exact_match_exists": (k, s) in exact_keys,
                "normalized_match_exists": (
                    normalize_product_name(k),
                    normalize_origin_name(s)
                ) in norm_keys,
            })

    demo_df = pd.DataFrame(rows)
    print_df_preview(demo_df, "Exact vs normalized match demo:", max_rows=50)


# ============================================================
# MAIN
# ============================================================
def main():
    report = ValidationReport()

    section("FRESH VEGGIE DATA VALIDATOR")

    data = validate_required_tables(report)
    if not data:
        report.error("No tables could be loaded")
        report.summary()
        return

    validate_required_columns(data, report)

    required_for_followup = ["E", "K", "S_imp", "S_cl", "WM", "M", "P_k", "C_pur_imp", "C_pur_cl", "C_pur_wm", "a_ksm"]
    if any(k not in data for k in required_for_followup):
        report.error("Missing required tables; stopping deeper validation")
        report.summary()
        return

    master = build_master_sets(data)

    validate_product_and_origin_consistency(data, master, report)
    validate_entry_point_types(master, report)
    validate_import_cost_matrix(data, master, report)
    validate_domestic_and_wm_costs(data, master, report)
    validate_availability_table(data, master, report)
    validate_selling_prices(data, master, report)
    print_exact_mismatch_demo(data, master)

    report.summary()


if __name__ == "__main__":
    main()