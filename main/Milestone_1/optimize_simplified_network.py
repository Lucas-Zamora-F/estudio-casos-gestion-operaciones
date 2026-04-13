import sqlite3
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
import pulp


# ============================================================
# FILE: optimize_simplified_network.py
# PURPOSE:
#   Solve the simplified network design model for Deliverable 1.
#
# MODEL:
#   max sum_z [ covered_population(z) * alpha_max *
#               sum_k (D_pc_ann(k) * P_k(k))
#               - sum_f (C_open(f) * b_fz(f,z)) ] * y_z
#
#   s.t. sum_z y_z = 1
#        y_z in {0,1}
#
# DATA SOURCES:
#   - Sets/model.db
#       * Z(z_name, ..., covered_population, covered_households)
#       * K(product)
#   - Sets/parameters.db
#       * C_open(facility, cost_usd)
#       * D_pc_ann(product, annual_per_capita_demand_kg)
#       * P_k(product, price_usd_per_kg)
#   - Sets/b_fz.db
#       * b_fz(facility, z_name, is_open)
# ============================================================


# -------------------------
# CONFIG
# -------------------------
MODEL_DB_PATH = Path("Sets/model.db")
PARAM_DB_PATH = Path("Sets/parameters.db")
BFZ_DB_PATH = Path("Sets/b_fz.db")

ALPHA_MAX = 0.20
OUTPUT_CSV = Path("Results/simplified_network_optimization_results.csv")


# -------------------------
# DATABASE HELPERS
# -------------------------
def read_table(db_path: Path, table_name: str) -> pd.DataFrame:
    """Read an entire SQLite table into a pandas DataFrame."""
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    with sqlite3.connect(db_path) as conn:
        return pd.read_sql_query(f"SELECT * FROM {table_name}", conn)


def validate_required_columns(df: pd.DataFrame, table_name: str, required_cols: List[str]) -> None:
    """Validate that a DataFrame contains the required columns."""
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(
            f"Table '{table_name}' is missing required columns: {missing}\n"
            f"Available columns: {list(df.columns)}"
        )


def print_unique_values(df: pd.DataFrame, column: str, table_name: str) -> None:
    """Print sorted unique values from a given column for debugging."""
    print(f"\n[DEBUG] Unique values in {table_name}.{column}:")
    values = (
        df[column]
        .dropna()
        .astype(str)
        .str.strip()
        .unique()
        .tolist()
    )
    values = sorted(values)
    for value in values:
        print(f" - {value}")
    print(f"[COUNT] {len(values)} unique values")


# -------------------------
# DATA LOADING
# -------------------------
def load_data() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load all required tables from the SQLite databases."""
    z_df = read_table(MODEL_DB_PATH, "Z")
    k_df = read_table(MODEL_DB_PATH, "K")

    c_open_df = read_table(PARAM_DB_PATH, "C_open")
    d_pc_df = read_table(PARAM_DB_PATH, "D_pc_ann")
    p_k_df = read_table(PARAM_DB_PATH, "P_k")

    b_fz_df = read_table(BFZ_DB_PATH, "b_fz")

    validate_required_columns(z_df, "Z", ["z_name", "covered_population"])
    validate_required_columns(k_df, "K", ["product"])
    validate_required_columns(c_open_df, "C_open", ["facility", "cost_usd"])
    validate_required_columns(d_pc_df, "D_pc_ann", ["product", "annual_per_capita_demand_kg"])
    validate_required_columns(p_k_df, "P_k", ["product", "price_usd_per_kg"])
    validate_required_columns(b_fz_df, "b_fz", ["facility", "z_name", "is_open"])

    return z_df, k_df, b_fz_df, c_open_df, d_pc_df, p_k_df


# -------------------------
# PARAMETER BUILDERS
# -------------------------
def build_revenue_per_person(
    k_df: pd.DataFrame,
    d_pc_df: pd.DataFrame,
    p_k_df: pd.DataFrame,
) -> float:
    """
    Compute:
        revenue_per_person = sum_k D_pc_ann(k) * P_k(k)

    Returns:
        float: annual revenue contribution per covered person before alpha_max.
    """
    products_df = k_df[["product"]].drop_duplicates().copy()
    demand_df = d_pc_df[["product", "annual_per_capita_demand_kg"]].copy()
    price_df = p_k_df[["product", "price_usd_per_kg"]].copy()

    # Keep a single price per product if duplicates exist
    if price_df["product"].duplicated().any():
        print("\n[WARN] Duplicate products found in P_k. Using mean price per product.")
        price_df = (
            price_df.groupby("product", as_index=False)["price_usd_per_kg"]
            .mean()
        )

    # Keep a single demand per product if duplicates exist
    if demand_df["product"].duplicated().any():
        print("\n[WARN] Duplicate products found in D_pc_ann. Using mean demand per product.")
        demand_df = (
            demand_df.groupby("product", as_index=False)["annual_per_capita_demand_kg"]
            .mean()
        )

    merged = products_df.merge(demand_df, on="product", how="left")
    merged = merged.merge(price_df, on="product", how="left")

    if merged["annual_per_capita_demand_kg"].isna().any():
        missing_products = merged.loc[
            merged["annual_per_capita_demand_kg"].isna(), "product"
        ].tolist()
        raise ValueError(
            f"Missing annual per-capita demand for products: {missing_products}"
        )

    if merged["price_usd_per_kg"].isna().any():
        missing_products = merged.loc[
            merged["price_usd_per_kg"].isna(), "product"
        ].tolist()
        raise ValueError(
            f"Missing selling price for products: {missing_products}"
        )

    merged["annual_revenue_per_person_product"] = (
        merged["annual_per_capita_demand_kg"] * merged["price_usd_per_kg"]
    )

    revenue_per_person = merged["annual_revenue_per_person_product"].sum()

    print("\n[INFO] Revenue components by product:")
    print(
        merged[
            [
                "product",
                "annual_per_capita_demand_kg",
                "price_usd_per_kg",
                "annual_revenue_per_person_product",
            ]
        ].to_string(index=False)
    )

    print(f"\n[INFO] Annual revenue per person (before alpha_max): {revenue_per_person:,.4f} USD/person/year")

    return float(revenue_per_person)


def build_opening_cost_by_z(
    z_df: pd.DataFrame,
    b_fz_df: pd.DataFrame,
    c_open_df: pd.DataFrame,
) -> Dict[str, float]:
    """
    Compute:
        opening_cost(z) = sum_f C_open(f) * b_fz(f,z)

    Returns:
        dict mapping z_name -> opening cost
    """
    merged = b_fz_df.merge(c_open_df, on="facility", how="left")

    if merged["cost_usd"].isna().any():
        missing_facilities = (
            merged.loc[merged["cost_usd"].isna(), "facility"]
            .drop_duplicates()
            .tolist()
        )
        raise ValueError(
            f"Missing opening cost for facilities: {missing_facilities}"
        )

    merged["is_open"] = merged["is_open"].astype(int)
    merged["open_cost_component"] = merged["is_open"] * merged["cost_usd"]

    open_cost_by_z_df = (
        merged.groupby("z_name", as_index=False)["open_cost_component"]
        .sum()
        .rename(columns={"open_cost_component": "opening_cost_usd"})
    )

    all_z = z_df[["z_name"]].drop_duplicates()
    open_cost_by_z_df = all_z.merge(open_cost_by_z_df, on="z_name", how="left")
    open_cost_by_z_df["opening_cost_usd"] = open_cost_by_z_df["opening_cost_usd"].fillna(0.0)

    return dict(zip(open_cost_by_z_df["z_name"], open_cost_by_z_df["opening_cost_usd"]))


def build_covered_population_by_z(z_df: pd.DataFrame) -> Dict[str, float]:
    """Return a dict mapping z_name -> covered_population."""
    z_clean = z_df[["z_name", "covered_population"]].drop_duplicates().copy()

    if z_clean["covered_population"].isna().any():
        missing = z_clean.loc[z_clean["covered_population"].isna(), "z_name"].tolist()
        raise ValueError(f"Missing covered_population for configurations: {missing}")

    return dict(zip(z_clean["z_name"], z_clean["covered_population"]))


def build_facilities_by_z(b_fz_df: pd.DataFrame) -> Dict[str, List[str]]:
    """Return a dict mapping z_name -> list of open facilities."""
    tmp = b_fz_df.copy()
    tmp["is_open"] = tmp["is_open"].astype(int)
    tmp = tmp[tmp["is_open"] == 1]

    grouped = (
        tmp.groupby("z_name")["facility"]
        .apply(list)
        .to_dict()
    )

    return grouped


# -------------------------
# OPTIMIZATION
# -------------------------
def solve_model(
    z_names: List[str],
    covered_population_by_z: Dict[str, float],
    opening_cost_by_z: Dict[str, float],
    annual_revenue_per_person: float,
    alpha_max: float,
) -> Tuple[str, Dict[str, float]]:
    """
    Solve the simplified MILP with PuLP.

    Returns:
        best_z_name, metrics_dict
    """
    model = pulp.LpProblem("Simplified_Network_Design", pulp.LpMaximize)

    y = pulp.LpVariable.dicts("y", z_names, lowBound=0, upBound=1, cat="Binary")

    revenue_expr = pulp.lpSum(
        covered_population_by_z[z] * alpha_max * annual_revenue_per_person * y[z]
        for z in z_names
    )

    opening_cost_expr = pulp.lpSum(
        opening_cost_by_z[z] * y[z]
        for z in z_names
    )

    model += revenue_expr - opening_cost_expr, "Total_Profit"

    model += pulp.lpSum(y[z] for z in z_names) == 1, "Select_Exactly_One_Configuration"

    solver = pulp.PULP_CBC_CMD(msg=False)
    model.solve(solver)

    status = pulp.LpStatus[model.status]
    if status != "Optimal":
        raise RuntimeError(f"Solver did not find an optimal solution. Status: {status}")

    selected = [z for z in z_names if pulp.value(y[z]) is not None and pulp.value(y[z]) > 0.5]
    if len(selected) != 1:
        raise RuntimeError(f"Expected exactly one selected configuration, got: {selected}")

    best_z = selected[0]
    revenue = covered_population_by_z[best_z] * alpha_max * annual_revenue_per_person
    opening_cost = opening_cost_by_z[best_z]
    objective_value = revenue - opening_cost

    metrics = {
        "objective_value_usd": objective_value,
        "revenue_usd": revenue,
        "opening_cost_usd": opening_cost,
        "covered_population": covered_population_by_z[best_z],
        "alpha_max": alpha_max,
    }

    return best_z, metrics


# -------------------------
# RANK ALL CONFIGURATIONS
# -------------------------
def rank_all_configurations(
    z_names: List[str],
    covered_population_by_z: Dict[str, float],
    opening_cost_by_z: Dict[str, float],
    annual_revenue_per_person: float,
    alpha_max: float,
    facilities_by_z: Dict[str, List[str]],
) -> pd.DataFrame:
    """Build a ranked table of all configurations."""
    rows = []

    for z in z_names:
        revenue = covered_population_by_z[z] * alpha_max * annual_revenue_per_person
        opening_cost = opening_cost_by_z[z]
        objective_value = revenue - opening_cost

        rows.append(
            {
                "z_name": z,
                "covered_population": covered_population_by_z[z],
                "alpha_max": alpha_max,
                "revenue_usd": revenue,
                "opening_cost_usd": opening_cost,
                "objective_value_usd": objective_value,
                "open_facilities": ", ".join(facilities_by_z.get(z, [])),
            }
        )

    results_df = pd.DataFrame(rows)
    results_df = results_df.sort_values(by="objective_value_usd", ascending=False).reset_index(drop=True)
    return results_df


# -------------------------
# MAIN
# -------------------------
def main() -> None:
    print("=" * 70)
    print("SIMPLIFIED NETWORK OPTIMIZATION - DELIVERABLE 1")
    print("=" * 70)

    print("\n[STEP 1] Loading data...")
    z_df, k_df, b_fz_df, c_open_df, d_pc_df, p_k_df = load_data()

    print(f"  - Loaded Z configurations: {len(z_df)} rows")
    print(f"  - Loaded K products: {len(k_df)} rows")
    print(f"  - Loaded b_fz rows: {len(b_fz_df)}")
    print(f"  - Loaded C_open rows: {len(c_open_df)}")
    print(f"  - Loaded D_pc_ann rows: {len(d_pc_df)}")
    print(f"  - Loaded P_k rows: {len(p_k_df)}")

    print("\n" + "=" * 70)
    print("DEBUG: UNIQUE PRODUCT NAMES")
    print("=" * 70)
    print_unique_values(k_df, "product", "K")
    print_unique_values(d_pc_df, "product", "D_pc_ann")
    print_unique_values(p_k_df, "product", "P_k")

    print("\n[STEP 2] Building model parameters...")
    annual_revenue_per_person = build_revenue_per_person(k_df, d_pc_df, p_k_df)
    covered_population_by_z = build_covered_population_by_z(z_df)
    opening_cost_by_z = build_opening_cost_by_z(z_df, b_fz_df, c_open_df)
    facilities_by_z = build_facilities_by_z(b_fz_df)

    z_names = sorted(list(covered_population_by_z.keys()))
    print(f"  - Number of candidate configurations: {len(z_names)}")
    print(f"  - alpha_max: {ALPHA_MAX}")

    print("\n[STEP 3] Solving optimization model...")
    best_z, metrics = solve_model(
        z_names=z_names,
        covered_population_by_z=covered_population_by_z,
        opening_cost_by_z=opening_cost_by_z,
        annual_revenue_per_person=annual_revenue_per_person,
        alpha_max=ALPHA_MAX,
    )

    print("\n[STEP 4] Ranking all configurations...")
    results_df = rank_all_configurations(
        z_names=z_names,
        covered_population_by_z=covered_population_by_z,
        opening_cost_by_z=opening_cost_by_z,
        annual_revenue_per_person=annual_revenue_per_person,
        alpha_max=ALPHA_MAX,
        facilities_by_z=facilities_by_z,
    )

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    results_df.to_csv(OUTPUT_CSV, index=False)

    best_row = results_df.iloc[0]

    print("\n" + "=" * 70)
    print("OPTIMAL SOLUTION")
    print("=" * 70)
    print(f"Optimal y_z = 1 for: {best_z}")
    print(f"Revenue (USD): {metrics['revenue_usd']:,.2f}")
    print(f"Opening cost (USD): {metrics['opening_cost_usd']:,.2f}")
    print(f"Objective value (USD): {metrics['objective_value_usd']:,.2f}")
    print(f"Covered population: {metrics['covered_population']:,.0f}")
    print(f"Open facilities: {best_row['open_facilities']}")

    print("\nTop 10 configurations:")
    print(
        results_df[
            [
                "z_name",
                "covered_population",
                "revenue_usd",
                "opening_cost_usd",
                "objective_value_usd",
            ]
        ].head(10).to_string(index=False)
    )

    print(f"\n[INFO] Full ranking saved to: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()