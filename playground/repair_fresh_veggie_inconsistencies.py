import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd


# ============================================================
# CONFIG
# ============================================================
MODEL_DB = Path("Sets/model.db")
PARAM_DB = Path("Sets/parameters.db")


# ============================================================
# HELPERS
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


def fetch_table(conn: sqlite3.Connection, table_name: str) -> pd.DataFrame:
    return pd.read_sql_query(f'SELECT * FROM "{table_name}"', conn)


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    q = """
    SELECT name
    FROM sqlite_master
    WHERE type='table' AND name=?
    """
    row = conn.execute(q, (table_name,)).fetchone()
    return row is not None


def column_exists(conn: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    q = f'PRAGMA table_info("{table_name}")'
    cols = [row[1] for row in conn.execute(q).fetchall()]
    return column_name in cols


# ============================================================
# CANONICAL MAPS
# ============================================================
CANONICAL_PRODUCTS = {
    "roma tomatoes": "Roma Tomatoes",
    "roma tomato": "Roma Tomatoes",
    "tomato roma": "Roma Tomatoes",

    "cauliflower": "Cauliflower",

    "broccoli": "Broccoli",
    "brocolli": "Broccoli",

    "asparagus": "Asparagus",

    "green bell pepper": "Green Bell Pepper",
    "green bell peppers": "Green Bell Pepper",
    "green pepper": "Green Bell Pepper",
    "bell pepper": "Green Bell Pepper",
    "pepper": "Green Bell Pepper",
}

CANONICAL_ORIGINS = {
    "us": "US",
    "usa": "US",
    "united states": "US",
    "united states of america": "US",

    "mexico": "Mexico",
    "peru": "Peru",
    "ecuador": "Ecuador",
    "bolivia": "Bolivia",
    "argentina": "Argentina",
    "spain": "Spain",

    "chile-scl": "Chile-Scl",
    "chile-vap": "Chile-Vap",
    "chile-maul": "Chile-Maul",
    "chile-coq": "Chile-Coq",

    "la vega central": "La Vega Central",
    "lo valledor": "Lo Valledor",
}


# ============================================================
# NORMALIZATION FUNCTIONS
# ============================================================
def canonical_product(x: Any) -> str:
    x0 = norm(x)
    return CANONICAL_PRODUCTS.get(x0, str(x).strip() if not pd.isna(x) else "")


def canonical_origin(x: Any) -> str:
    x0 = norm(x)
    return CANONICAL_ORIGINS.get(x0, str(x).strip() if not pd.isna(x) else "")


# ============================================================
# AUDIT HELPERS
# ============================================================
def preview_distinct_values(
    conn: sqlite3.Connection,
    table_name: str,
    column_name: str,
    title: str,
) -> None:
    if not table_exists(conn, table_name) or not column_exists(conn, table_name, column_name):
        log(f"{title}: table/column not found")
        return

    q = f'''
    SELECT DISTINCT "{column_name}"
    FROM "{table_name}"
    ORDER BY "{column_name}"
    '''
    rows = [r[0] for r in conn.execute(q).fetchall()]
    log(title)
    for r in rows:
        print(f"- {r}")


def detect_duplicate_keys_after_canonicalization(
    conn: sqlite3.Connection,
    table_name: str,
    key_cols: List[str],
    product_col: str = None,
    origin_col: str = None,
) -> List[Tuple]:
    """
    Reads the full table in pandas, canonicalizes product/origin if provided,
    and checks whether multiple rows collapse into the same canonical key.
    """
    df = fetch_table(conn, table_name)

    if df.empty:
        return []

    df2 = df.copy()

    if product_col and product_col in df2.columns:
        df2[product_col] = df2[product_col].apply(canonical_product)

    if origin_col and origin_col in df2.columns:
        df2[origin_col] = df2[origin_col].apply(canonical_origin)

    grouped = df2.groupby(key_cols, dropna=False).size().reset_index(name="count")
    dup = grouped[grouped["count"] > 1].copy()

    if dup.empty:
        return []

    return [tuple(row[col] for col in key_cols) + (int(row["count"]),) for _, row in dup.iterrows()]


# ============================================================
# SQL UPDATE ROUTINES
# ============================================================
def repair_column_with_map(
    conn: sqlite3.Connection,
    table_name: str,
    column_name: str,
    mapper_fn,
) -> int:
    """
    Updates one text column row by row only if canonical value differs.
    """
    if not table_exists(conn, table_name):
        log(f"Skipping {table_name}.{column_name}: table not found")
        return 0

    if not column_exists(conn, table_name, column_name):
        log(f"Skipping {table_name}.{column_name}: column not found")
        return 0

    df = fetch_table(conn, table_name)
    if df.empty:
        log(f"Skipping {table_name}.{column_name}: empty table")
        return 0

    # Need rowid to update safely
    df = pd.read_sql_query(f'SELECT rowid AS _rowid_, * FROM "{table_name}"', conn)

    updates = []
    for _, row in df.iterrows():
        old_val = row[column_name]
        new_val = mapper_fn(old_val)

        old_clean = "" if pd.isna(old_val) else str(old_val).strip()
        new_clean = "" if pd.isna(new_val) else str(new_val).strip()

        if old_clean != new_clean:
            updates.append((new_clean, int(row["_rowid_"]), old_clean, new_clean))

    for new_clean, rowid, _, _ in updates:
        conn.execute(
            f'UPDATE "{table_name}" SET "{column_name}" = ? WHERE rowid = ?',
            (new_clean, rowid),
        )

    if updates:
        log(f"Updated {len(updates)} rows in {table_name}.{column_name}")
        for _, _, old_clean, new_clean in updates[:20]:
            log(f"  {old_clean} -> {new_clean}")
        if len(updates) > 20:
            log(f"  ... {len(updates) - 20} more changes")
    else:
        log(f"No changes needed in {table_name}.{column_name}")

    return len(updates)


# ============================================================
# REPAIR PLAN
# ============================================================
def repair_model_db(conn: sqlite3.Connection) -> int:
    section("REPAIRING MODEL.DB")
    total = 0

    # Master tables
    total += repair_column_with_map(conn, "K", "product", canonical_product)
    total += repair_column_with_map(conn, "S_imp", "origin", canonical_origin)
    total += repair_column_with_map(conn, "S_cl", "origin", canonical_origin)
    total += repair_column_with_map(conn, "WM", "wholesale_market", canonical_origin)

    return total


def repair_param_db(conn: sqlite3.Connection) -> int:
    section("REPAIRING PARAMETERS.DB")
    total = 0

    # Prices / demand / availability / purchase costs
    total += repair_column_with_map(conn, "P_k", "product", canonical_product)

    total += repair_column_with_map(conn, "C_pur_imp", "product", canonical_product)
    total += repair_column_with_map(conn, "C_pur_imp", "origin", canonical_origin)

    total += repair_column_with_map(conn, "C_pur_cl", "product", canonical_product)
    total += repair_column_with_map(conn, "C_pur_cl", "origin", canonical_origin)

    total += repair_column_with_map(conn, "C_pur_wm", "product", canonical_product)

    total += repair_column_with_map(conn, "D_pc_ann", "product", canonical_product)

    total += repair_column_with_map(conn, "a_ksm", "product", canonical_product)
    total += repair_column_with_map(conn, "a_ksm", "origin", canonical_origin)

    return total


# ============================================================
# PRE-CHECKS
# ============================================================
def run_prechecks(model_conn: sqlite3.Connection, param_conn: sqlite3.Connection) -> None:
    section("PRE-CHECKS BEFORE REPAIR")

    preview_distinct_values(model_conn, "K", "product", "MODEL.K product values:")
    preview_distinct_values(model_conn, "S_imp", "origin", "MODEL.S_imp origin values:")
    preview_distinct_values(param_conn, "C_pur_imp", "product", "PARAM.C_pur_imp product values:")
    preview_distinct_values(param_conn, "C_pur_imp", "origin", "PARAM.C_pur_imp origin values:")

    # Duplicate safety checks
    log("\nChecking for duplicate collapse risk after canonicalization...")

    dup_checks = [
        (
            param_conn,
            "C_pur_imp",
            ["product", "origin"],
            "product",
            "origin",
        ),
        (
            param_conn,
            "C_pur_cl",
            ["product", "origin"],
            "product",
            "origin",
        ),
        (
            param_conn,
            "a_ksm",
            ["product", "origin"],
            "product",
            "origin",
        ),
        (
            param_conn,
            "P_k",
            ["product"],
            "product",
            None,
        ),
        (
            param_conn,
            "C_pur_wm",
            ["product"],
            "product",
            None,
        ),
        (
            model_conn,
            "K",
            ["product"],
            "product",
            None,
        ),
        (
            model_conn,
            "S_imp",
            ["origin"],
            None,
            "origin",
        ),
    ]

    problems = []
    for conn, table_name, key_cols, product_col, origin_col in dup_checks:
        if not table_exists(conn, table_name):
            continue

        dup = detect_duplicate_keys_after_canonicalization(
            conn=conn,
            table_name=table_name,
            key_cols=key_cols,
            product_col=product_col,
            origin_col=origin_col,
        )
        if dup:
            problems.append((table_name, dup[:20]))

    if problems:
        log("\n[ERROR] Canonicalization would create duplicates. Fix these first.")
        for table_name, dup in problems:
            log(f"\nTable: {table_name}")
            for item in dup:
                log(f"  {item}")
        raise RuntimeError("Repair aborted due to duplicate collapse risk.")

    log("No duplicate collapse risk detected.")


# ============================================================
# POST-CHECKS
# ============================================================
def run_postchecks(model_conn: sqlite3.Connection, param_conn: sqlite3.Connection) -> None:
    section("POST-CHECKS AFTER REPAIR")

    preview_distinct_values(model_conn, "K", "product", "MODEL.K product values after repair:")
    preview_distinct_values(model_conn, "S_imp", "origin", "MODEL.S_imp origin values after repair:")
    preview_distinct_values(param_conn, "C_pur_imp", "product", "PARAM.C_pur_imp product values after repair:")
    preview_distinct_values(param_conn, "C_pur_imp", "origin", "PARAM.C_pur_imp origin values after repair:")

    # Direct exact-match check against validator findings
    if table_exists(model_conn, "K") and table_exists(model_conn, "S_imp") and table_exists(param_conn, "C_pur_imp"):
        df_k = fetch_table(model_conn, "K")
        df_simp = fetch_table(model_conn, "S_imp")
        df_imp = fetch_table(param_conn, "C_pur_imp")

        k_set = set(df_k["product"].astype(str).str.strip())
        s_set = set(df_simp["origin"].astype(str).str.strip())
        imp_prod_set = set(df_imp["product"].astype(str).str.strip())
        imp_org_set = set(df_imp["origin"].astype(str).str.strip())

        missing_products = sorted(k_set - imp_prod_set)
        missing_origins = sorted(s_set - imp_org_set)

        if missing_products:
            log(f"[WARNING] Products still missing exactly in C_pur_imp: {missing_products}")
        else:
            log("All products in K now match exactly with C_pur_imp.")

        if missing_origins:
            log(f"[WARNING] Origins still missing exactly in C_pur_imp: {missing_origins}")
        else:
            log("All import origins in S_imp now match exactly with C_pur_imp.")


# ============================================================
# MAIN
# ============================================================
def main():
    section("FRESH VEGGIE - REPAIR INCONSISTENCIES")

    if not MODEL_DB.exists():
        raise FileNotFoundError(f"MODEL_DB not found: {MODEL_DB}")

    if not PARAM_DB.exists():
        raise FileNotFoundError(f"PARAM_DB not found: {PARAM_DB}")

    model_conn = sqlite3.connect(MODEL_DB)
    param_conn = sqlite3.connect(PARAM_DB)

    try:
        run_prechecks(model_conn, param_conn)

        section("APPLYING REPAIRS")
        model_conn.execute("BEGIN")
        param_conn.execute("BEGIN")

        total_model = repair_model_db(model_conn)
        total_param = repair_param_db(param_conn)

        model_conn.commit()
        param_conn.commit()

        log(f"\nCommitted changes.")
        log(f"Total updates in model.db      : {total_model}")
        log(f"Total updates in parameters.db : {total_param}")
        log(f"Grand total updates            : {total_model + total_param}")

        run_postchecks(model_conn, param_conn)

    except Exception as e:
        model_conn.rollback()
        param_conn.rollback()
        log(f"\n[ERROR] Repair failed. Rolled back all changes.")
        log(str(e))
        raise
    finally:
        model_conn.close()
        param_conn.close()


if __name__ == "__main__":
    main()