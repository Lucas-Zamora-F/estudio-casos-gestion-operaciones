import sqlite3
from pathlib import Path
from typing import Dict, List, Tuple, Set

import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import pulp

from shapely.geometry import Point
from shapely.ops import unary_union


# ============================================================
# FILE:
#   optimize_simplified_network_by_scenario.py
#
# PURPOSE:
#   Solve the simplified network design model for Deliverable 1
#   under 4 different scenarios:
#       1) DC only
#       2) DC + DS
#       3) DC + MDCP
#       4) DC + DS + MDCP
#
#   Then generate 4 coverage plots, one for the optimal
#   configuration of each scenario.
# ============================================================


# =============================
# CONFIG
# =============================
MODEL_DB_PATH = Path("Sets/model.db")
PARAM_DB_PATH = Path("Sets/parameters.db")
BFZ_DB_PATH = Path("Sets/b_fz.db")

OUTPUT_DIR = Path("main/Milestone_1")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_SUMMARY_CSV = OUTPUT_DIR / "simplified_network_scenarios_summary.csv"
OUTPUT_FULL_CSV = OUTPUT_DIR / "simplified_network_all_rankings_by_scenario.csv"

SHAPE_PATH = Path("extern data/DPA 2024/DPA 2024/COMUNAS/COMUNAS_v1.shp")
if not SHAPE_PATH.exists():
    SHAPE_PATH = Path("extern data/DPA 2024/COMUNAS/COMUNAS_v1.shp")

CENSUS_PATH = Path("extern data/CENSO/Base_manzana_entidad_CPV24.csv")

ALPHA_MAX = 0.20

DC_RADIUS = 15000
DS_RADIUS = 5000
MDCP_RADIUS = 10000

PERSONS_PER_HOUSEHOLD = 2.8
ZOOM_PADDING_M = 8000

BUFFER_COLORS = [
    "#e41a1c", "#ff7f00", "#ffff33", "#f781bf", "#a65628",
    "#ffffff", "#00ffff", "#ff1493", "#ffd700", "#8b0000",
    "#00ced1", "#ff4500", "#f4a460", "#fffacd", "#dc143c",
    "#20b2aa", "#ff69b4", "#ffa500", "#ffe4b5",
]


# =============================
# DATABASE HELPERS
# =============================
def read_table(db_path: Path, table_name: str) -> pd.DataFrame:
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    with sqlite3.connect(db_path) as conn:
        return pd.read_sql_query(f"SELECT * FROM {table_name}", conn)


def validate_required_columns(df: pd.DataFrame, table_name: str, required_cols: List[str]) -> None:
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(
            f"Table '{table_name}' is missing required columns: {missing}\n"
            f"Available columns: {list(df.columns)}"
        )


def print_unique_values(df: pd.DataFrame, column: str, table_name: str) -> None:
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


# =============================
# GEOGRAPHY HELPERS
# =============================
def create_buffer(lon: float, lat: float, radius_m: float):
    point = gpd.GeoSeries([Point(lon, lat)], crs="EPSG:4326").to_crs(epsg=32719)
    return point.iloc[0].buffer(radius_m)


def project_point_xy(lon: float, lat: float):
    point = gpd.GeoSeries([Point(lon, lat)], crs="EPSG:4326").to_crs(epsg=32719)
    geom = point.iloc[0]
    return geom.x, geom.y


def prepare_geography() -> gpd.GeoDataFrame:
    if not SHAPE_PATH.exists():
        raise FileNotFoundError(f"Shapefile not found: {SHAPE_PATH}")
    if not CENSUS_PATH.exists():
        raise FileNotFoundError(f"Census file not found: {CENSUS_PATH}")

    gdf = gpd.read_file(SHAPE_PATH)

    gdf["CUT_REG"] = gdf["CUT_REG"].astype(str).str.zfill(2)
    gdf["CUT_COM"] = gdf["CUT_COM"].astype(str).str.zfill(5)
    gdf = gdf[gdf["CUT_REG"] == "13"].copy()

    df = pd.read_csv(CENSUS_PATH, sep=";", low_memory=False)
    df["COD_REGION"] = pd.to_numeric(df["COD_REGION"], errors="coerce")
    df = df[df["COD_REGION"] == 13].copy()
    df["CUT"] = df["CUT"].astype(str).str.zfill(5)

    df_com = (
        df.groupby("CUT", as_index=False)["n_per"]
        .sum()
        .rename(columns={"n_per": "POP"})
    )

    gdf = gdf.merge(df_com, left_on="CUT_COM", right_on="CUT", how="left")
    gdf["POP"] = gdf["POP"].fillna(0.0)
    gdf["HOUSEHOLDS"] = gdf["POP"] / PERSONS_PER_HOUSEHOLD

    return gdf.to_crs(epsg=32719)


def build_color_map(facility_names: List[str]) -> Dict[str, str]:
    color_map = {}
    for i, facility in enumerate(facility_names):
        color_map[facility] = BUFFER_COLORS[i % len(BUFFER_COLORS)]
    return color_map


def get_zoom_bounds(active_facilities: List[str], buffers: Dict[str, object], padding_m: int = 8000):
    if not active_facilities:
        return None

    geom_union = unary_union([buffers[f] for f in active_facilities])
    minx, miny, maxx, maxy = geom_union.bounds

    return (
        minx - padding_m,
        maxx + padding_m,
        miny - padding_m,
        maxy + padding_m,
    )


def get_radius_by_type(facility_type: str) -> float:
    if facility_type == "DC":
        return DC_RADIUS
    if facility_type == "DS":
        return DS_RADIUS
    if facility_type == "MDCP":
        return MDCP_RADIUS
    raise ValueError(f"Unknown facility type: {facility_type}")


# =============================
# DATA LOADING
# =============================
def load_data() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    z_df = read_table(MODEL_DB_PATH, "Z")
    k_df = read_table(MODEL_DB_PATH, "K")
    f_df = read_table(MODEL_DB_PATH, "F")

    c_open_df = read_table(PARAM_DB_PATH, "C_open")
    d_pc_df = read_table(PARAM_DB_PATH, "D_pc_ann")
    p_k_df = read_table(PARAM_DB_PATH, "P_k")

    b_fz_df = read_table(BFZ_DB_PATH, "b_fz")

    validate_required_columns(z_df, "Z", ["z_name", "covered_population", "covered_households"])
    validate_required_columns(k_df, "K", ["product"])
    validate_required_columns(f_df, "F", ["facility_name", "type", "latitude", "longitude"])
    validate_required_columns(c_open_df, "C_open", ["facility", "cost_usd"])
    validate_required_columns(d_pc_df, "D_pc_ann", ["product", "annual_per_capita_demand_kg"])
    validate_required_columns(p_k_df, "P_k", ["product", "price_usd_per_kg"])
    validate_required_columns(b_fz_df, "b_fz", ["facility", "z_name", "is_open"])

    return z_df, k_df, f_df, b_fz_df, c_open_df, d_pc_df, p_k_df


# =============================
# PARAMETER BUILDERS
# =============================
def build_revenue_per_person(
    k_df: pd.DataFrame,
    d_pc_df: pd.DataFrame,
    p_k_df: pd.DataFrame,
) -> float:
    products_df = k_df[["product"]].drop_duplicates().copy()
    demand_df = d_pc_df[["product", "annual_per_capita_demand_kg"]].copy()
    price_df = p_k_df[["product", "price_usd_per_kg"]].copy()

    if price_df["product"].duplicated().any():
        print("\n[WARN] Duplicate products found in P_k. Using mean price per product.")
        price_df = price_df.groupby("product", as_index=False)["price_usd_per_kg"].mean()

    if demand_df["product"].duplicated().any():
        print("\n[WARN] Duplicate products found in D_pc_ann. Using mean demand per product.")
        demand_df = demand_df.groupby("product", as_index=False)["annual_per_capita_demand_kg"].mean()

    merged = products_df.merge(demand_df, on="product", how="left")
    merged = merged.merge(price_df, on="product", how="left")

    if merged["annual_per_capita_demand_kg"].isna().any():
        missing_products = merged.loc[
            merged["annual_per_capita_demand_kg"].isna(), "product"
        ].tolist()
        raise ValueError(f"Missing annual per-capita demand for products: {missing_products}")

    if merged["price_usd_per_kg"].isna().any():
        missing_products = merged.loc[
            merged["price_usd_per_kg"].isna(), "product"
        ].tolist()
        raise ValueError(f"Missing selling price for products: {missing_products}")

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
    merged = b_fz_df.merge(c_open_df, on="facility", how="left")

    if merged["cost_usd"].isna().any():
        missing_facilities = (
            merged.loc[merged["cost_usd"].isna(), "facility"]
            .drop_duplicates()
            .tolist()
        )
        raise ValueError(f"Missing opening cost for facilities: {missing_facilities}")

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


def build_coverage_by_z(z_df: pd.DataFrame) -> Dict[str, Dict[str, float]]:
    z_clean = z_df[["z_name", "covered_population", "covered_households"]].drop_duplicates().copy()

    if z_clean["covered_population"].isna().any():
        missing = z_clean.loc[z_clean["covered_population"].isna(), "z_name"].tolist()
        raise ValueError(f"Missing covered_population for configurations: {missing}")

    if z_clean["covered_households"].isna().any():
        missing = z_clean.loc[z_clean["covered_households"].isna(), "z_name"].tolist()
        raise ValueError(f"Missing covered_households for configurations: {missing}")

    return {
        row["z_name"]: {
            "covered_population": float(row["covered_population"]),
            "covered_households": float(row["covered_households"]),
        }
        for _, row in z_clean.iterrows()
    }


def build_facilities_by_z(b_fz_df: pd.DataFrame) -> Dict[str, List[str]]:
    tmp = b_fz_df.copy()
    tmp["is_open"] = tmp["is_open"].astype(int)
    tmp = tmp[tmp["is_open"] == 1]
    return tmp.groupby("z_name")["facility"].apply(list).to_dict()


def build_facility_types_by_z(b_fz_df: pd.DataFrame, f_df: pd.DataFrame) -> Dict[str, Set[str]]:
    facility_type_map = dict(zip(f_df["facility_name"], f_df["type"]))

    tmp = b_fz_df.copy()
    tmp["is_open"] = tmp["is_open"].astype(int)
    tmp = tmp[tmp["is_open"] == 1].copy()
    tmp["facility_type"] = tmp["facility"].map(facility_type_map)

    if tmp["facility_type"].isna().any():
        missing = tmp.loc[tmp["facility_type"].isna(), "facility"].drop_duplicates().tolist()
        raise ValueError(f"Facilities in b_fz not found in F table: {missing}")

    return tmp.groupby("z_name")["facility_type"].apply(set).to_dict()


# =============================
# SCENARIO LOGIC
# =============================
def is_z_feasible_for_scenario(facility_types: Set[str], scenario_name: str) -> bool:
    allowed_types = {"DC", "DS", "MDCP"}

    if not facility_types.issubset(allowed_types):
        return False

    if "DC" not in facility_types:
        return False

    if scenario_name == "DC_ONLY":
        return facility_types == {"DC"}

    if scenario_name == "DC_DS":
        return facility_types.issubset({"DC", "DS"}) and ("DS" in facility_types)

    if scenario_name == "DC_MDCP":
        return facility_types.issubset({"DC", "MDCP"}) and ("MDCP" in facility_types)

    if scenario_name == "ALL":
        return facility_types == {"DC", "DS", "MDCP"}

    raise ValueError(f"Unknown scenario: {scenario_name}")


def get_feasible_z_for_scenario(
    z_names: List[str],
    facility_types_by_z: Dict[str, Set[str]],
    scenario_name: str,
) -> List[str]:
    feasible_z = []
    for z in z_names:
        facility_types = facility_types_by_z.get(z, set())
        if is_z_feasible_for_scenario(facility_types, scenario_name):
            feasible_z.append(z)
    return feasible_z


def scenario_title(scenario_name: str) -> str:
    mapping = {
        "DC_ONLY": "Optimal - DC Only",
        "DC_DS": "Optimal - DC + DS",
        "DC_MDCP": "Optimal - DC + MDCP",
        "ALL": "Optimal - DC + DS + MDCP",
    }
    return mapping.get(scenario_name, scenario_name)


# =============================
# OPTIMIZATION
# =============================
def solve_model_for_scenario(
    scenario_name: str,
    feasible_z_names: List[str],
    coverage_by_z: Dict[str, Dict[str, float]],
    opening_cost_by_z: Dict[str, float],
    annual_revenue_per_person: float,
    alpha_max: float,
) -> Tuple[str, Dict[str, float]]:
    if not feasible_z_names:
        raise RuntimeError(f"No feasible configurations found for scenario {scenario_name}")

    model = pulp.LpProblem(f"Simplified_Network_Design_{scenario_name}", pulp.LpMaximize)

    y = pulp.LpVariable.dicts("y", feasible_z_names, lowBound=0, upBound=1, cat="Binary")

    revenue_expr = pulp.lpSum(
        coverage_by_z[z]["covered_population"] * alpha_max * annual_revenue_per_person * y[z]
        for z in feasible_z_names
    )

    opening_cost_expr = pulp.lpSum(
        opening_cost_by_z[z] * y[z]
        for z in feasible_z_names
    )

    model += revenue_expr - opening_cost_expr, "Total_Profit"
    model += pulp.lpSum(y[z] for z in feasible_z_names) == 1, "Select_Exactly_One_Configuration"

    solver = pulp.PULP_CBC_CMD(msg=False)
    model.solve(solver)

    status = pulp.LpStatus[model.status]
    if status != "Optimal":
        raise RuntimeError(f"Solver did not find an optimal solution for {scenario_name}. Status: {status}")

    selected = [z for z in feasible_z_names if pulp.value(y[z]) is not None and pulp.value(y[z]) > 0.5]
    if len(selected) != 1:
        raise RuntimeError(f"Expected exactly one selected configuration in {scenario_name}, got: {selected}")

    best_z = selected[0]
    revenue = coverage_by_z[best_z]["covered_population"] * alpha_max * annual_revenue_per_person
    opening_cost = opening_cost_by_z[best_z]
    objective_value = revenue - opening_cost

    metrics = {
        "scenario": scenario_name,
        "objective_value_usd": objective_value,
        "revenue_usd": revenue,
        "opening_cost_usd": opening_cost,
        "covered_population": coverage_by_z[best_z]["covered_population"],
        "covered_households": coverage_by_z[best_z]["covered_households"],
        "alpha_max": alpha_max,
        "n_feasible_configurations": len(feasible_z_names),
    }

    return best_z, metrics


def rank_configurations_for_scenario(
    scenario_name: str,
    feasible_z_names: List[str],
    coverage_by_z: Dict[str, Dict[str, float]],
    opening_cost_by_z: Dict[str, float],
    annual_revenue_per_person: float,
    alpha_max: float,
    facilities_by_z: Dict[str, List[str]],
) -> pd.DataFrame:
    rows = []

    for z in feasible_z_names:
        revenue = coverage_by_z[z]["covered_population"] * alpha_max * annual_revenue_per_person
        opening_cost = opening_cost_by_z[z]
        objective_value = revenue - opening_cost

        rows.append(
            {
                "scenario": scenario_name,
                "z_name": z,
                "covered_population": coverage_by_z[z]["covered_population"],
                "covered_households": coverage_by_z[z]["covered_households"],
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


# =============================
# PLOTTING
# =============================
def plot_optimal_scenario_coverage(
    scenario_name: str,
    z_name: str,
    metrics: Dict[str, float],
    f_df: pd.DataFrame,
    facilities_by_z: Dict[str, List[str]],
    gdf: gpd.GeoDataFrame,
) -> Path:
    active_facilities = facilities_by_z.get(z_name, [])
    if not active_facilities:
        raise ValueError(f"No active facilities found for {z_name}")

    buffers = {}
    projected_xy = {}

    for _, row in f_df.iterrows():
        facility_name = row["facility_name"]
        lon = float(row["longitude"])
        lat = float(row["latitude"])
        facility_type = row["type"]

        radius = get_radius_by_type(facility_type)
        buffers[facility_name] = create_buffer(lon, lat, radius)
        projected_xy[facility_name] = project_point_xy(lon, lat)

    facility_color_map = build_color_map(f_df["facility_name"].tolist())

    fig, ax = plt.subplots(figsize=(10, 10))

    gdf.plot(
        column="HOUSEHOLDS",
        ax=ax,
        legend=True,
        cmap="viridis",
        linewidth=0.5,
        edgecolor="black"
    )

    legend_handles = []
    legend_labels = []

    for facility in active_facilities:
        color = facility_color_map[facility]

        gpd.GeoSeries([buffers[facility]], crs="EPSG:32719").plot(
            ax=ax,
            facecolor=color,
            edgecolor=color,
            alpha=0.22,
            linewidth=2
        )

    for facility in active_facilities:
        x, y = projected_xy[facility]
        color = facility_color_map[facility]

        scatter = ax.scatter(x, y, color=color, edgecolors="black", s=90, zorder=5)

        ax.text(
            x,
            y,
            f" {facility}",
            fontsize=8,
            zorder=6,
            bbox=dict(facecolor="white", alpha=0.7, edgecolor="none", pad=1.5)
        )

        legend_handles.append(scatter)
        legend_labels.append(facility)

    bounds = get_zoom_bounds(active_facilities, buffers, padding_m=ZOOM_PADDING_M)
    if bounds is not None:
        minx, maxx, miny, maxy = bounds
        ax.set_xlim(minx, maxx)
        ax.set_ylim(miny, maxy)

    title = (
        f"{scenario_title(scenario_name)}\n"
        f"{z_name}\n"
        f"Covered population: {metrics['covered_population']:,.0f} | "
        f"Covered households: {metrics['covered_households']:,.0f}\n"
        f"Revenue: ${metrics['revenue_usd']:,.0f} | "
        f"Installation cost: ${metrics['opening_cost_usd']:,.0f} | "
        f"Objective: ${metrics['objective_value_usd']:,.0f}"
    )

    ax.set_title(title, fontsize=11)

    ax.legend(legend_handles, legend_labels, loc="upper left", fontsize=8)

    ax.set_axis_off()
    plt.tight_layout()

    output_path = OUTPUT_DIR / f"{scenario_name.lower()}_optimal_coverage.png"
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()

    return output_path


# =============================
# MAIN
# =============================
def main() -> None:
    print("=" * 70)
    print("SIMPLIFIED NETWORK OPTIMIZATION BY SCENARIO - DELIVERABLE 1")
    print("=" * 70)

    print("\n[STEP 1] Loading data...")
    z_df, k_df, f_df, b_fz_df, c_open_df, d_pc_df, p_k_df = load_data()

    print(f"  - Loaded Z configurations: {len(z_df)} rows")
    print(f"  - Loaded K products: {len(k_df)} rows")
    print(f"  - Loaded F facilities: {len(f_df)} rows")
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

    print("\n[STEP 2] Preparing geography...")
    gdf = prepare_geography()

    print("\n[STEP 3] Building model parameters...")
    annual_revenue_per_person = build_revenue_per_person(k_df, d_pc_df, p_k_df)
    coverage_by_z = build_coverage_by_z(z_df)
    opening_cost_by_z = build_opening_cost_by_z(z_df, b_fz_df, c_open_df)
    facilities_by_z = build_facilities_by_z(b_fz_df)
    facility_types_by_z = build_facility_types_by_z(b_fz_df, f_df)

    z_names = sorted(list(coverage_by_z.keys()))
    print(f"  - Number of candidate configurations: {len(z_names)}")
    print(f"  - alpha_max: {ALPHA_MAX}")

    scenarios = ["DC_ONLY", "DC_DS", "DC_MDCP", "ALL"]

    summary_rows = []
    all_rankings = []

    print("\n[STEP 4] Solving one optimization per scenario and plotting results...")

    for scenario in scenarios:
        print("\n" + "-" * 70)
        print(f"SCENARIO: {scenario}")
        print("-" * 70)

        feasible_z_names = get_feasible_z_for_scenario(
            z_names=z_names,
            facility_types_by_z=facility_types_by_z,
            scenario_name=scenario,
        )

        print(f"Feasible configurations in {scenario}: {len(feasible_z_names)}")

        best_z, metrics = solve_model_for_scenario(
            scenario_name=scenario,
            feasible_z_names=feasible_z_names,
            coverage_by_z=coverage_by_z,
            opening_cost_by_z=opening_cost_by_z,
            annual_revenue_per_person=annual_revenue_per_person,
            alpha_max=ALPHA_MAX,
        )

        scenario_ranking_df = rank_configurations_for_scenario(
            scenario_name=scenario,
            feasible_z_names=feasible_z_names,
            coverage_by_z=coverage_by_z,
            opening_cost_by_z=opening_cost_by_z,
            annual_revenue_per_person=annual_revenue_per_person,
            alpha_max=ALPHA_MAX,
            facilities_by_z=facilities_by_z,
        )

        best_row = scenario_ranking_df.iloc[0]

        print(f"Optimal y_z = 1 for: {best_z}")
        print(f"Revenue (USD): {metrics['revenue_usd']:,.2f}")
        print(f"Opening cost (USD): {metrics['opening_cost_usd']:,.2f}")
        print(f"Objective value (USD): {metrics['objective_value_usd']:,.2f}")
        print(f"Covered population: {metrics['covered_population']:,.0f}")
        print(f"Covered households: {metrics['covered_households']:,.0f}")
        print(f"Open facilities: {best_row['open_facilities']}")

        print("\nTop 5 configurations in this scenario:")
        print(
            scenario_ranking_df[
                [
                    "z_name",
                    "covered_population",
                    "covered_households",
                    "revenue_usd",
                    "opening_cost_usd",
                    "objective_value_usd",
                ]
            ].head(5).to_string(index=False)
        )

        plot_path = plot_optimal_scenario_coverage(
            scenario_name=scenario,
            z_name=best_z,
            metrics=metrics,
            f_df=f_df,
            facilities_by_z=facilities_by_z,
            gdf=gdf,
        )

        print(f"Coverage plot saved to: {plot_path}")

        summary_rows.append(
            {
                "scenario": scenario,
                "optimal_z_name": best_z,
                "covered_population": metrics["covered_population"],
                "covered_households": metrics["covered_households"],
                "revenue_usd": metrics["revenue_usd"],
                "opening_cost_usd": metrics["opening_cost_usd"],
                "objective_value_usd": metrics["objective_value_usd"],
                "n_feasible_configurations": metrics["n_feasible_configurations"],
                "open_facilities": best_row["open_facilities"],
                "coverage_plot_path": str(plot_path),
            }
        )

        all_rankings.append(scenario_ranking_df)

    summary_df = pd.DataFrame(summary_rows).sort_values(by="objective_value_usd", ascending=False).reset_index(drop=True)
    all_rankings_df = pd.concat(all_rankings, ignore_index=True)

    summary_df.to_csv(OUTPUT_SUMMARY_CSV, index=False)
    all_rankings_df.to_csv(OUTPUT_FULL_CSV, index=False)

    print("\n" + "=" * 70)
    print("FINAL SCENARIO COMPARISON")
    print("=" * 70)
    print(summary_df.to_string(index=False))

    best_scenario_row = summary_df.iloc[0]
    print("\nBest scenario overall:")
    print(f"Scenario: {best_scenario_row['scenario']}")
    print(f"Optimal z: {best_scenario_row['optimal_z_name']}")
    print(f"Revenue (USD): {best_scenario_row['revenue_usd']:,.2f}")
    print(f"Opening cost (USD): {best_scenario_row['opening_cost_usd']:,.2f}")
    print(f"Objective value (USD): {best_scenario_row['objective_value_usd']:,.2f}")

    print(f"\n[INFO] Scenario summary saved to: {OUTPUT_SUMMARY_CSV}")
    print(f"[INFO] Full rankings saved to: {OUTPUT_FULL_CSV}")
    print(f"[INFO] Coverage plots saved to: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()