import sqlite3
import time
from pathlib import Path
from typing import Dict, Set, List
import pandas as pd
import pulp
from tqdm import tqdm


# ============================================================
# CONFIG
# ============================================================
MODEL_DB = Path("Sets/model.db")
PARAM_DB = Path("Sets/parameters.db")
TRANSPORT_DB = Path("Sets/transport_matrices.db")

OUTPUT_DIR = Path("main/optimization_results")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------------
# Hardcoded model constants
# -----------------------------
ALPHA_MAX = 0.20

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

SOLVER_TIME_LIMIT_SEC = 600
SOLVER_GAP_REL = 0.0001
VERBOSE_SOLVER = True


# ============================================================
# LOGGING HELPERS
# ============================================================
def log(msg: str) -> None:
    now = time.strftime("%H:%M:%S")
    print(f"[{now}] {msg}")


def section(title: str) -> None:
    print("\n" + "=" * 90)
    print(title)
    print("=" * 90)


class Timer:
    def __init__(self, label: str):
        self.label = label
        self.start = None

    def __enter__(self):
        log(f"START -> {self.label}")
        self.start = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        elapsed = time.time() - self.start
        log(f"END   -> {self.label} | elapsed = {elapsed:.2f} s")


# ============================================================
# UTILITIES
# ============================================================
def normalize_text(x) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip()


def fetch_table(db_path: Path, table_name: str) -> pd.DataFrame:
    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query(f'SELECT * FROM "{table_name}"', conn)
    finally:
        conn.close()
    return df


def safe_float(x, default=0.0) -> float:
    try:
        if pd.isna(x):
            return default
        return float(x)
    except Exception:
        return default


def safe_int(x, default=0) -> int:
    try:
        if pd.isna(x):
            return default
        return int(x)
    except Exception:
        return default


def scenario_allowed_types(scenario_name: str) -> Set[str]:
    if scenario_name == "dc_only":
        return {"DC"}
    if scenario_name == "dc_ds":
        return {"DC", "DS"}
    if scenario_name == "dc_mdcp":
        return {"DC", "MDCP"}
    if scenario_name == "all":
        return {"DC", "DS", "MDCP"}
    raise ValueError(f"Unknown scenario: {scenario_name}")


def month_col_from_name(month_name: str) -> str:
    return f"available_{month_name}"


def var_value(v) -> float:
    val = pulp.value(v)
    if val is None:
        return 0.0
    return float(val)


# ============================================================
# DATA LOADING
# ============================================================
def load_all_data() -> Dict[str, pd.DataFrame]:
    section("LOADING ALL TABLES FROM SQLITE")

    table_sources = [
        ("E", MODEL_DB, "E"),
        ("F", MODEL_DB, "F"),
        ("K", MODEL_DB, "K"),
        ("P_F_i", MODEL_DB, "P_F_i"),
        ("S_cl", MODEL_DB, "S_cl"),
        ("S_imp", MODEL_DB, "S_imp"),
        ("WM", MODEL_DB, "WM"),
        ("Z", MODEL_DB, "Z"),
        ("C_open", PARAM_DB, "C_open"),
        ("C_pur_cl", PARAM_DB, "C_pur_cl"),
        ("C_pur_imp", PARAM_DB, "C_pur_imp"),
        ("C_pur_wm", PARAM_DB, "C_pur_wm"),
        ("D_MDC", PARAM_DB, "D_MDC"),
        ("D_pc_ann", PARAM_DB, "D_pc_ann"),
        ("M", PARAM_DB, "M"),
        ("P_k", PARAM_DB, "P_k"),
        ("a_ksm", PARAM_DB, "a_ksm"),
        ("DC_MDCP_distance", TRANSPORT_DB, "DC_MDCP_distance"),
        ("DC_MDCP_time", TRANSPORT_DB, "DC_MDCP_time"),
        ("DC_SD_distance", TRANSPORT_DB, "DC_SD_distance"),
        ("DC_SD_time", TRANSPORT_DB, "DC_SD_time"),
        ("E_DC_distance", TRANSPORT_DB, "E_DC_distance"),
        ("E_DC_time", TRANSPORT_DB, "E_DC_time"),
        ("MDCP_distance", TRANSPORT_DB, "MDCP_distance"),
        ("MDCP_time", TRANSPORT_DB, "MDCP_time"),
        ("S_cl_DC_distance", TRANSPORT_DB, "S_cl_DC_distance"),
        ("S_cl_DC_time", TRANSPORT_DB, "S_cl_DC_time"),
        ("WM_CD_distance", TRANSPORT_DB, "WM_CD_distance"),
        ("WM_CD_time", TRANSPORT_DB, "WM_CD_time"),
    ]

    data = {}
    for key, db_path, table_name in tqdm(table_sources, desc="Loading tables", unit="table"):
        df = fetch_table(db_path, table_name)
        for col in df.columns:
            if pd.api.types.is_object_dtype(df[col]):
                df[col] = df[col].apply(normalize_text)
        data[key] = df
        log(f"Loaded {key:<18} | rows={len(df):>8} | cols={len(df.columns):>3}")

    return data


# ============================================================
# PREPROCESSING
# ============================================================
def preprocess(data: Dict[str, pd.DataFrame]) -> Dict:
    section("PREPROCESSING DATA")

    with Timer("Building sets and dictionaries"):
        df_E = data["E"].copy()
        df_F = data["F"].copy()
        df_K = data["K"].copy()
        df_PFi = data["P_F_i"].copy()
        df_Scl = data["S_cl"].copy()
        df_Simp = data["S_imp"].copy()
        df_WM = data["WM"].copy()
        df_Z = data["Z"].copy()
        df_Copen = data["C_open"].copy()
        df_Cpurcl = data["C_pur_cl"].copy()
        df_Cpurimp = data["C_pur_imp"].copy()
        df_Cpurwm = data["C_pur_wm"].copy()
        df_DMDC = data["D_MDC"].copy()
        df_Dpcann = data["D_pc_ann"].copy()
        df_M = data["M"].copy()
        df_Pk = data["P_k"].copy()
        df_aksm = data["a_ksm"].copy()

        log("Building base sets...")
        E = sorted(df_E["international entry point"].tolist())
        F = sorted(df_F["facility_name"].tolist())
        K = sorted(df_K["product"].tolist())
        S_cl = sorted(df_Scl["origin"].tolist())
        S_imp = sorted(df_Simp["origin"].tolist())
        WM = sorted(df_WM["wholesale_market"].tolist())

        months_df = df_M.sort_values("month_num").copy()
        M = months_df["month_name"].tolist()

        log("Building facility types...")
        facility_type = {
            row["facility_name"]: row["type"]
            for _, row in df_F.iterrows()
        }

        DC = sorted([f for f in F if facility_type.get(f) == "DC"])
        DS = sorted([f for f in F if facility_type.get(f) == "DS"])
        MDCP = sorted([f for f in F if facility_type.get(f) == "MDCP"])

        z_list = sorted(df_Z["z_name"].tolist())

        log("Building entry point types...")
        e_type = {
            row["international entry point"]: row["type"]
            for _, row in df_E.iterrows()
        }

        log("Building facility coverage p_i...")
        p_i = {}
        for _, row in df_PFi.iterrows():
            p_i[row["facility_name"]] = safe_float(row["covered_population"], 0.0)

        log("Building configuration coverage p_z...")
        p_z = {}
        h_z = {}
        for _, row in df_Z.iterrows():
            z = row["z_name"]
            p_z[z] = safe_float(row["covered_population"], 0.0)
            h_z[z] = safe_float(row["covered_households"], 0.0)

        log("Mapping facility binary columns from Z...")
        z_binary_cols = [c for c in df_Z.columns if c in F]
        missing_facility_cols = [f for f in F if f not in z_binary_cols]
        if missing_facility_cols:
            raise ValueError(
                "These facilities are in F but not as binary columns in Z: "
                + ", ".join(missing_facility_cols)
            )

        log(f"Facility binary columns found in Z: {len(z_binary_cols)}")

        log("Building C_open...")
        C_open = {}
        for _, row in df_Copen.iterrows():
            C_open[row["facility"]] = safe_float(row["cost_usd"], 0.0)
        for f in F:
            C_open.setdefault(f, 0.0)

        log("Building product selling prices P_k...")
        P_k = {}
        for _, row in df_Pk.iterrows():
            P_k[row["product"]] = safe_float(row["price_usd_per_kg"], 0.0)

        log("Building monthly per-capita demand d_pc from annual...")
        D_pc_ann = {}
        d_pc = {}
        for _, row in df_Dpcann.iterrows():
            k = row["product"]
            annual = safe_float(row["annual_per_capita_demand_kg"], 0.0)
            D_pc_ann[k] = annual
            for m in M:
                d_pc[(k, m)] = annual / 12.0

        log("Building availability a_ksm...")
        a_ksm = {}
        for _, row in df_aksm.iterrows():
            s = row["origin"]
            k = row["product"]
            for m in M:
                col = month_col_from_name(m)
                a_ksm[(k, s, m)] = safe_int(row.get(col, 0), 0)

        for k in K:
            for s in (S_cl + S_imp):
                for m in M:
                    a_ksm.setdefault((k, s, m), 0)

        log("Building purchase costs C_pur_cl...")
        C_pur_cl = {}
        for _, row in df_Cpurcl.iterrows():
            C_pur_cl[(row["product"], row["origin"])] = safe_float(row["cost_usd_per_kg"], 0.0)

        log("Building purchase costs C_pur_wm...")
        C_pur_wm = {}
        wm_product_cost = {}
        for _, row in df_Cpurwm.iterrows():
            wm_product_cost[row["product"]] = safe_float(row["cost_usd_per_kg"], 0.0)
        for k in K:
            for w in WM:
                C_pur_wm[(k, w)] = wm_product_cost.get(k, 0.0)

        log("Building imported purchase costs C_pur_imp by entry point type...")
        C_pur_imp = {}
        raw_imp_cost = {}
        for _, row in df_Cpurimp.iterrows():
            raw_imp_cost[(row["product"], row["origin"])] = {
                "Port": safe_float(row["purchase_cost_usd_per_kg_sea"], 0.0),
                "Airport": safe_float(row["purchase_cost_usd_per_kg_air"], 0.0),
                "Land customs": safe_float(row["purchase_cost_usd_per_kg_land"], 0.0),
            }

        for k in tqdm(K, desc="Building import purchase costs", unit="product"):
            for s in S_imp:
                for e in E:
                    e_t = e_type.get(e, "")
                    cost = raw_imp_cost.get((k, s), {}).get(e_t, 0.0)
                    C_pur_imp[(k, s, e)] = cost

        def build_distance_dict(df: pd.DataFrame, label: str) -> Dict:
            d = {}
            for _, row in tqdm(df.iterrows(), total=len(df), desc=label, unit="row"):
                d[(row["origin_name"], row["destination_name"])] = safe_float(row["distance_km"], 0.0)
            return d

        log("Building distance dictionaries...")
        D_E_DC = build_distance_dict(data["E_DC_distance"], "Distance E->DC")
        D_Scl_DC = build_distance_dict(data["S_cl_DC_distance"], "Distance S_cl->DC")
        D_WM_DC = build_distance_dict(data["WM_CD_distance"], "Distance WM->DC")
        D_DC_DS = build_distance_dict(data["DC_SD_distance"], "Distance DC->DS")
        D_DC_MDCP = build_distance_dict(data["DC_MDCP_distance"], "Distance DC->MDCP")

        log("Building average last-mile distances D_lm...")
        D_lm = {}
        for f in F:
            ft = facility_type.get(f)
            if ft == "DC":
                D_lm[f] = (2.0 / 3.0) * 15.0
            elif ft == "DS":
                D_lm[f] = (2.0 / 3.0) * 5.0
            elif ft == "MDCP":
                D_lm[f] = (2.0 / 3.0) * 10.0
            else:
                D_lm[f] = 0.0

        log("Building D_MDC...")
        D_MDC = {}
        for _, row in tqdm(df_DMDC.iterrows(), total=len(df_DMDC), desc="Building D_MDC", unit="row"):
            z = row["z_name"]
            start_cd = row["start_cd"]
            D_MDC[(z, start_cd)] = safe_float(row["min_distance_km"], 0.0)

        for z in z_list:
            for i in DC:
                D_MDC.setdefault((z, i), 0.0)

        log("Building Big-M values...")
        max_pz = max(p_z.values()) if len(p_z) > 0 else 0.0

        M_net = {}
        M_imp = {}
        M_cl = {}
        M_del = {}
        M_DS = {}
        M_MDC = {}

        for k in K:
            for m in M:
                M_net[(k, m)] = ALPHA_MAX * max_pz * d_pc[(k, m)]
                M_MDC[(k, m)] = M_net[(k, m)]

        for k in tqdm(K, desc="Building M_imp", unit="product"):
            for s in S_imp:
                for e in E:
                    for m in M:
                        M_imp[(k, s, e, m)] = M_net[(k, m)]

        for k in tqdm(K, desc="Building M_cl", unit="product"):
            for s in S_cl:
                for m in M:
                    M_cl[(k, s, m)] = M_net[(k, m)]

        for k in tqdm(K, desc="Building M_del", unit="product"):
            for i in F:
                for m in M:
                    M_del[(k, i, m)] = ALPHA_MAX * p_i.get(i, 0.0) * d_pc[(k, m)]

        for k in tqdm(K, desc="Building M_DS", unit="product"):
            for j in DS:
                for m in M:
                    M_DS[(k, j, m)] = ALPHA_MAX * p_i.get(j, 0.0) * d_pc[(k, m)]

    log(f"Counts -> |E|={len(E)}, |F|={len(F)}, |DC|={len(DC)}, |DS|={len(DS)}, |MDCP|={len(MDCP)}")
    log(f"Counts -> |K|={len(K)}, |S_cl|={len(S_cl)}, |S_imp|={len(S_imp)}, |WM|={len(WM)}, |M|={len(M)}, |Z|={len(z_list)}")

    return {
        "E": E,
        "F": F,
        "K": K,
        "S_cl": S_cl,
        "S_imp": S_imp,
        "WM": WM,
        "M": M,
        "DC": DC,
        "DS": DS,
        "MDCP": MDCP,
        "Z": z_list,
        "Z_df": df_Z,
        "z_binary_cols": z_binary_cols,
        "facility_type": facility_type,
        "e_type": e_type,
        "p_i": p_i,
        "p_z": p_z,
        "h_z": h_z,
        "C_open": C_open,
        "P_k": P_k,
        "d_pc": d_pc,
        "a_ksm": a_ksm,
        "C_pur_cl": C_pur_cl,
        "C_pur_wm": C_pur_wm,
        "C_pur_imp": C_pur_imp,
        "D_E_DC": D_E_DC,
        "D_Scl_DC": D_Scl_DC,
        "D_WM_DC": D_WM_DC,
        "D_DC_DS": D_DC_DS,
        "D_DC_MDCP": D_DC_MDCP,
        "D_lm": D_lm,
        "D_MDC": D_MDC,
        "M_imp": M_imp,
        "M_cl": M_cl,
        "M_del": M_del,
        "M_DS": M_DS,
        "M_MDC": M_MDC,
    }


# ============================================================
# SCENARIO FILTERING
# ============================================================
def filter_scenario(base: Dict, scenario_name: str) -> Dict:
    section(f"FILTERING SCENARIO -> {scenario_name}")

    allowed_types = scenario_allowed_types(scenario_name)
    log(f"Allowed facility types: {sorted(list(allowed_types))}")

    F_s = [f for f in base["F"] if base["facility_type"][f] in allowed_types]
    DC_s = [f for f in base["DC"] if f in F_s]
    DS_s = [f for f in base["DS"] if f in F_s]
    MDCP_s = [f for f in base["MDCP"] if f in F_s]

    log("Filtering Z directly from binary columns in Z table...")
    Z_df = base["Z_df"].copy()

    disallowed_facilities = [f for f in base["F"] if base["facility_type"][f] not in allowed_types]

    if len(disallowed_facilities) > 0:
        # keep only rows where all disallowed facilities are 0
        mask = pd.Series(True, index=Z_df.index)
        for f in tqdm(disallowed_facilities, desc=f"Filtering configs for {scenario_name}", unit="facility"):
            mask &= (Z_df[f].fillna(0).astype(int) == 0)
        Z_df_s = Z_df.loc[mask].copy()
    else:
        Z_df_s = Z_df.copy()

    Z_s = sorted(Z_df_s["z_name"].tolist())

    log(f"Scenario {scenario_name}: |F|={len(F_s)}, |DC|={len(DC_s)}, |DS|={len(DS_s)}, |MDCP|={len(MDCP_s)}, |Z|={len(Z_s)}")

    filtered = base.copy()
    filtered["F"] = F_s
    filtered["DC"] = DC_s
    filtered["DS"] = DS_s
    filtered["MDCP"] = MDCP_s
    filtered["Z"] = Z_s
    filtered["Z_df"] = Z_df_s

    return filtered


# ============================================================
# MODEL BUILDING
# ============================================================
def build_model(data: Dict, scenario_name: str):
    section(f"BUILDING MODEL -> {scenario_name}")

    E = data["E"]
    F = data["F"]
    K = data["K"]
    S_cl = data["S_cl"]
    S_imp = data["S_imp"]
    WM = data["WM"]
    M = data["M"]
    DC = data["DC"]
    DS = data["DS"]
    MDCP = data["MDCP"]
    Z = data["Z"]

    p_i = data["p_i"]
    p_z = data["p_z"]
    C_open = data["C_open"]
    P_k = data["P_k"]
    d_pc = data["d_pc"]
    a_ksm = data["a_ksm"]
    C_pur_cl = data["C_pur_cl"]
    C_pur_wm = data["C_pur_wm"]
    C_pur_imp = data["C_pur_imp"]
    D_E_DC = data["D_E_DC"]
    D_Scl_DC = data["D_Scl_DC"]
    D_WM_DC = data["D_WM_DC"]
    D_DC_DS = data["D_DC_DS"]
    D_lm = data["D_lm"]
    D_MDC = data["D_MDC"]

    M_imp = data["M_imp"]
    M_cl = data["M_cl"]
    M_del = data["M_del"]
    M_DS = data["M_DS"]
    M_MDC = data["M_MDC"]

    Z_df = data["Z_df"].copy()
    Z_df = Z_df.set_index("z_name", drop=False)

    def b_fz(facility: str, z_name: str) -> int:
        return safe_int(Z_df.at[z_name, facility], 0)

    with Timer(f"Create PuLP model object [{scenario_name}]"):
        model = pulp.LpProblem(f"FreshVeggie_{scenario_name}", pulp.LpMaximize)

    with Timer(f"Create decision variables [{scenario_name}]"):
        y = pulp.LpVariable.dicts("y", Z, lowBound=0, upBound=1, cat=pulp.LpBinary)

        q_imp = pulp.LpVariable.dicts(
            "q_imp",
            [(k, s, e, m) for k in K for s in S_imp for e in E for m in M],
            lowBound=0,
            cat=pulp.LpContinuous,
        )

        q_cl = pulp.LpVariable.dicts(
            "q_cl",
            [(k, s, m) for k in K for s in S_cl for m in M],
            lowBound=0,
            cat=pulp.LpContinuous,
        )

        q_wm = pulp.LpVariable.dicts(
            "q_wm",
            [(k, w, m) for k in K for w in WM for m in M],
            lowBound=0,
            cat=pulp.LpContinuous,
        )

        x_E_DC = pulp.LpVariable.dicts(
            "x_E_DC",
            [(k, e, i, m) for k in K for e in E for i in DC for m in M],
            lowBound=0,
            cat=pulp.LpContinuous,
        )

        x_Scl_DC = pulp.LpVariable.dicts(
            "x_Scl_DC",
            [(k, s, i, m) for k in K for s in S_cl for i in DC for m in M],
            lowBound=0,
            cat=pulp.LpContinuous,
        )

        x_WM_DC = pulp.LpVariable.dicts(
            "x_WM_DC",
            [(k, w, i, m) for k in K for w in WM for i in DC for m in M],
            lowBound=0,
            cat=pulp.LpContinuous,
        )

        x_DC_DS = pulp.LpVariable.dicts(
            "x_DC_DS",
            [(k, i, j, m) for k in K for i in DC for j in DS for m in M],
            lowBound=0,
            cat=pulp.LpContinuous,
        )

        x_DC_MDC = pulp.LpVariable.dicts(
            "x_DC_MDC",
            [(k, i, m) for k in K for i in DC for m in M],
            lowBound=0,
            cat=pulp.LpContinuous,
        )

        x_F_C = pulp.LpVariable.dicts(
            "x_F_C",
            [(k, i, m) for k in K for i in F for m in M],
            lowBound=0,
            cat=pulp.LpContinuous,
        )

        u_zim = pulp.LpVariable.dicts(
            "u_zim",
            [(z, i, m) for z in Z for i in DC for m in M],
            lowBound=0,
            upBound=1,
            cat=pulp.LpBinary,
        )

    log(
        f"Variable blocks created -> "
        f"y={len(y)}, "
        f"q_imp={len(q_imp)}, q_cl={len(q_cl)}, q_wm={len(q_wm)}, "
        f"x_E_DC={len(x_E_DC)}, x_Scl_DC={len(x_Scl_DC)}, x_WM_DC={len(x_WM_DC)}, "
        f"x_DC_DS={len(x_DC_DS)}, x_DC_MDC={len(x_DC_MDC)}, x_F_C={len(x_F_C)}, "
        f"u_zim={len(u_zim)}"
    )

    with Timer(f"Build objective [{scenario_name}]"):
        revenue = pulp.lpSum(
            P_k[k] * x_F_C[(k, i, m)]
            for k in K for i in F for m in M
        )

        acquisition_cost = pulp.lpSum(
            C_pur_imp[(k, s, e)] * q_imp[(k, s, e, m)]
            for k in K for s in S_imp for e in E for m in M
        ) + pulp.lpSum(
            C_pur_cl.get((k, s), 0.0) * q_cl[(k, s, m)]
            for k in K for s in S_cl for m in M
        ) + pulp.lpSum(
            C_pur_wm.get((k, w), 0.0) * q_wm[(k, w, m)]
            for k in K for w in WM for m in M
        )

        longhaul_cost = pulp.lpSum(
            C_TR_LONG[k] * D_E_DC.get((e, i), 0.0) * x_E_DC[(k, e, i, m)]
            for k in K for e in E for i in DC for m in M
        ) + pulp.lpSum(
            C_TR_LONG[k] * D_Scl_DC.get((s, i), 0.0) * x_Scl_DC[(k, s, i, m)]
            for k in K for s in S_cl for i in DC for m in M
        ) + pulp.lpSum(
            C_TR_LONG[k] * D_WM_DC.get((w, i), 0.0) * x_WM_DC[(k, w, i, m)]
            for k in K for w in WM for i in DC for m in M
        )

        interfacility_cost = pulp.lpSum(
            C_TR_INT[k] * D_DC_DS.get((i, j), 0.0) * x_DC_DS[(k, i, j, m)]
            for k in K for i in DC for j in DS for m in M
        )

        lastmile_cost = pulp.lpSum(
            C_LM[k] * D_lm[i] * x_F_C[(k, i, m)]
            for k in K for i in F for m in M
        )

        facility_opening_cost = pulp.lpSum(
            C_open.get(f, 0.0) * b_fz(f, z) * y[z]
            for z in Z for f in F
        )

        mdc_movement_cost = pulp.lpSum(
            C_MOV * D_MDC.get((z, i), 0.0) * u_zim[(z, i, m)]
            for z in Z for i in DC for m in M
        )

        model += (
            revenue
            - acquisition_cost
            - longhaul_cost
            - interfacility_cost
            - lastmile_cost
            - mdc_movement_cost
            - facility_opening_cost
        ), "Total_Profit"

    section(f"ADDING CONSTRAINTS -> {scenario_name}")

    with Timer(f"Add config selection constraint [{scenario_name}]"):
        model += pulp.lpSum(y[z] for z in Z) == 1, "Select_One_Config"

    with Timer(f"Add availability constraints [{scenario_name}]"):
        for k in tqdm(K, desc=f"{scenario_name} avail imp", unit="product"):
            for s in S_imp:
                for e in E:
                    for m in M:
                        model += (
                            q_imp[(k, s, e, m)] <= M_imp[(k, s, e, m)] * a_ksm[(k, s, m)],
                            f"Avail_imp__{k}__{s}__{e}__{m}"
                        )

        for k in tqdm(K, desc=f"{scenario_name} avail cl", unit="product"):
            for s in S_cl:
                for m in M:
                    model += (
                        q_cl[(k, s, m)] <= M_cl[(k, s, m)] * a_ksm[(k, s, m)],
                        f"Avail_cl__{k}__{s}__{m}"
                    )

    with Timer(f"Add purchase-flow consistency [{scenario_name}]"):
        for k in tqdm(K, desc=f"{scenario_name} purchase-flow imp", unit="product"):
            for e in E:
                for m in M:
                    model += (
                        pulp.lpSum(x_E_DC[(k, e, i, m)] for i in DC)
                        == pulp.lpSum(q_imp[(k, s, e, m)] for s in S_imp),
                        f"PurchaseFlow_imp__{k}__{e}__{m}"
                    )

        for k in tqdm(K, desc=f"{scenario_name} purchase-flow cl", unit="product"):
            for s in S_cl:
                for m in M:
                    model += (
                        pulp.lpSum(x_Scl_DC[(k, s, i, m)] for i in DC)
                        == q_cl[(k, s, m)],
                        f"PurchaseFlow_cl__{k}__{s}__{m}"
                    )

        for k in tqdm(K, desc=f"{scenario_name} purchase-flow wm", unit="product"):
            for w in WM:
                for m in M:
                    model += (
                        pulp.lpSum(x_WM_DC[(k, w, i, m)] for i in DC)
                        == q_wm[(k, w, m)],
                        f"PurchaseFlow_wm__{k}__{w}__{m}"
                    )

    with Timer(f"Add DC flow balance [{scenario_name}]"):
        for k in tqdm(K, desc=f"{scenario_name} DC balance", unit="product"):
            for i in DC:
                for m in M:
                    model += (
                        pulp.lpSum(x_E_DC[(k, e, i, m)] for e in E)
                        + pulp.lpSum(x_Scl_DC[(k, s, i, m)] for s in S_cl)
                        + pulp.lpSum(x_WM_DC[(k, w, i, m)] for w in WM)
                        ==
                        pulp.lpSum(x_DC_DS[(k, i, j, m)] for j in DS)
                        + x_DC_MDC[(k, i, m)]
                        + x_F_C[(k, i, m)],
                        f"FlowBalance_DC__{k}__{i}__{m}"
                    )

    with Timer(f"Add DS flow balance [{scenario_name}]"):
        for k in tqdm(K, desc=f"{scenario_name} DS balance", unit="product"):
            for j in DS:
                for m in M:
                    model += (
                        pulp.lpSum(x_DC_DS[(k, i, j, m)] for i in DC)
                        == x_F_C[(k, j, m)],
                        f"FlowBalance_DS__{k}__{j}__{m}"
                    )

    with Timer(f"Add activation constraints [{scenario_name}]"):
        for k in tqdm(K, desc=f"{scenario_name} activation deliveries", unit="product"):
            for i in F:
                for m in M:
                    model += (
                        x_F_C[(k, i, m)] <= M_del[(k, i, m)] * pulp.lpSum(b_fz(i, z) * y[z] for z in Z),
                        f"Activation_del__{k}__{i}__{m}"
                    )

        for k in tqdm(K, desc=f"{scenario_name} activation DS", unit="product"):
            for j in DS:
                for m in M:
                    model += (
                        pulp.lpSum(x_DC_DS[(k, i, j, m)] for i in DC)
                        <= M_DS[(k, j, m)] * pulp.lpSum(b_fz(j, z) * y[z] for z in Z),
                        f"Activation_DS__{k}__{j}__{m}"
                    )

    with Timer(f"Add demand constraints [{scenario_name}]"):
        for k in tqdm(K, desc=f"{scenario_name} demand network", unit="product"):
            for m in M:
                model += (
                    pulp.lpSum(x_F_C[(k, i, m)] for i in F)
                    <= ALPHA_MAX * pulp.lpSum(p_z[z] * d_pc[(k, m)] * y[z] for z in Z),
                    f"Demand_network__{k}__{m}"
                )

        for k in tqdm(K, desc=f"{scenario_name} demand facility", unit="product"):
            for i in F:
                for m in M:
                    model += (
                        x_F_C[(k, i, m)]
                        <= ALPHA_MAX * d_pc[(k, m)] * p_i.get(i, 0.0) * pulp.lpSum(b_fz(i, z) * y[z] for z in Z),
                        f"Demand_facility__{k}__{i}__{m}"
                    )

    with Timer(f"Add MDC constraints [{scenario_name}]"):
        if len(MDCP) > 0:
            for k in tqdm(K, desc=f"{scenario_name} MDC load balance", unit="product"):
                for m in M:
                    model += (
                        pulp.lpSum(x_DC_MDC[(k, i, m)] for i in DC)
                        == pulp.lpSum(x_F_C[(k, n, m)] for n in MDCP),
                        f"MDC_load_balance__{k}__{m}"
                    )

            for m in tqdm(M, desc=f"{scenario_name} MDC one start", unit="month"):
                model += (
                    pulp.lpSum(u_zim[(z, i, m)] for z in Z for i in DC) == 1,
                    f"MDC_one_start_cd_per_month__{m}"
                )

            for z in tqdm(Z, desc=f"{scenario_name} MDC u<=y", unit="config"):
                for i in DC:
                    for m in M:
                        model += (
                            u_zim[(z, i, m)] <= y[z],
                            f"MDC_u_leq_y__{z}__{i}__{m}"
                        )

            for z in tqdm(Z, desc=f"{scenario_name} MDC open CD", unit="config"):
                for i in DC:
                    for m in M:
                        model += (
                            u_zim[(z, i, m)] <= b_fz(i, z),
                            f"MDC_u_leq_open_cd__{z}__{i}__{m}"
                        )

            for k in tqdm(K, desc=f"{scenario_name} MDC activation", unit="product"):
                for i in DC:
                    for m in M:
                        model += (
                            x_DC_MDC[(k, i, m)]
                            <= M_MDC[(k, m)] * pulp.lpSum(u_zim[(z, i, m)] for z in Z),
                            f"MDC_load_activation__{k}__{i}__{m}"
                        )
        else:
            for k in tqdm(K, desc=f"{scenario_name} No MDC flow", unit="product"):
                for i in DC:
                    for m in M:
                        model += x_DC_MDC[(k, i, m)] == 0, f"No_MDC_flow__{k}__{i}__{m}"

            for z in tqdm(Z, desc=f"{scenario_name} No MDC u", unit="config"):
                for i in DC:
                    for m in M:
                        model += u_zim[(z, i, m)] == 0, f"No_MDC_u__{z}__{i}__{m}"

    log(f"Model built successfully for scenario {scenario_name}")
    log(f"Variables: {len(model.variables())}")
    log(f"Constraints: {len(model.constraints)}")

    return {
        "model": model,
        "vars": {
            "y": y,
            "q_imp": q_imp,
            "q_cl": q_cl,
            "q_wm": q_wm,
            "x_E_DC": x_E_DC,
            "x_Scl_DC": x_Scl_DC,
            "x_WM_DC": x_WM_DC,
            "x_DC_DS": x_DC_DS,
            "x_DC_MDC": x_DC_MDC,
            "x_F_C": x_F_C,
            "u_zim": u_zim,
        }
    }


# ============================================================
# SOLVING
# ============================================================
def solve_model(model: pulp.LpProblem, scenario_name: str) -> None:
    section(f"SOLVING MODEL -> {scenario_name}")
    log(f"Solver = CBC | timeLimit = {SOLVER_TIME_LIMIT_SEC}s | gapRel = {SOLVER_GAP_REL}")

    solver = pulp.PULP_CBC_CMD(
        msg=VERBOSE_SOLVER,
        timeLimit=SOLVER_TIME_LIMIT_SEC,
        gapRel=SOLVER_GAP_REL,
    )

    with Timer(f"CBC solve [{scenario_name}]"):
        model.solve(solver)

    log(f"Solver status code: {model.status}")
    log(f"Solver status text: {pulp.LpStatus[model.status]}")


# ============================================================
# RESULT EXTRACTION
# ============================================================
def extract_results(data: Dict, model_obj: Dict, scenario_name: str) -> Dict[str, pd.DataFrame]:
    section(f"EXTRACTING RESULTS -> {scenario_name}")

    model = model_obj["model"]
    vars_ = model_obj["vars"]

    y = vars_["y"]
    q_imp = vars_["q_imp"]
    q_cl = vars_["q_cl"]
    q_wm = vars_["q_wm"]
    x_E_DC = vars_["x_E_DC"]
    x_Scl_DC = vars_["x_Scl_DC"]
    x_WM_DC = vars_["x_WM_DC"]
    x_DC_DS = vars_["x_DC_DS"]
    x_DC_MDC = vars_["x_DC_MDC"]
    x_F_C = vars_["x_F_C"]
    u_zim = vars_["u_zim"]

    Z_df = data["Z_df"].copy()
    Z_df = Z_df.set_index("z_name", drop=False)

    def b_fz(facility: str, z_name: str) -> int:
        return safe_int(Z_df.at[z_name, facility], 0)

    status = pulp.LpStatus[model.status]
    objective_value = pulp.value(model.objective)

    selected_z = [z for z, var in y.items() if var_value(var) > 0.5]
    selected_z_name = selected_z[0] if selected_z else None
    covered_population = data["p_z"].get(selected_z_name, 0.0) if selected_z_name else 0.0

    summary_df = pd.DataFrame([{
        "scenario": scenario_name,
        "status": status,
        "objective_value": objective_value,
        "selected_z": selected_z_name,
        "covered_population": covered_population,
    }])

    config_rows = []
    for z, var in tqdm(y.items(), desc=f"{scenario_name} config rows", unit="row"):
        val = var_value(var)
        if val > 1e-8:
            config_rows.append({
                "scenario": scenario_name,
                "z_name": z,
                "y_value": val,
                "covered_population": data["p_z"].get(z, 0.0),
            })
    config_df = pd.DataFrame(config_rows)

    open_rows = []
    if selected_z_name is not None:
        for f in tqdm(data["F"], desc=f"{scenario_name} open facilities", unit="facility"):
            is_open = b_fz(f, selected_z_name)
            if is_open == 1:
                open_rows.append({
                    "scenario": scenario_name,
                    "z_name": selected_z_name,
                    "facility": f,
                    "facility_type": data["facility_type"][f],
                    "covered_population": data["p_i"].get(f, 0.0),
                    "open_cost_usd": data["C_open"].get(f, 0.0),
                })
    open_facilities_df = pd.DataFrame(open_rows)

    purchase_rows = []
    for (k, s, e, m), var in tqdm(q_imp.items(), desc=f"{scenario_name} purchases import", unit="row"):
        val = var_value(var)
        if val > 1e-8:
            purchase_rows.append({
                "scenario": scenario_name,
                "purchase_type": "import",
                "product": k,
                "origin": s,
                "entry_point": e,
                "month": m,
                "quantity_kg": val,
                "unit_cost_usd_per_kg": data["C_pur_imp"][(k, s, e)],
                "total_purchase_cost_usd": val * data["C_pur_imp"][(k, s, e)],
            })

    for (k, s, m), var in tqdm(q_cl.items(), desc=f"{scenario_name} purchases domestic", unit="row"):
        val = var_value(var)
        if val > 1e-8:
            unit_cost = data["C_pur_cl"].get((k, s), 0.0)
            purchase_rows.append({
                "scenario": scenario_name,
                "purchase_type": "domestic",
                "product": k,
                "origin": s,
                "entry_point": None,
                "month": m,
                "quantity_kg": val,
                "unit_cost_usd_per_kg": unit_cost,
                "total_purchase_cost_usd": val * unit_cost,
            })

    for (k, w, m), var in tqdm(q_wm.items(), desc=f"{scenario_name} purchases wm", unit="row"):
        val = var_value(var)
        if val > 1e-8:
            unit_cost = data["C_pur_wm"].get((k, w), 0.0)
            purchase_rows.append({
                "scenario": scenario_name,
                "purchase_type": "wholesale_market",
                "product": k,
                "origin": w,
                "entry_point": None,
                "month": m,
                "quantity_kg": val,
                "unit_cost_usd_per_kg": unit_cost,
                "total_purchase_cost_usd": val * unit_cost,
            })

    purchases_df = pd.DataFrame(purchase_rows)

    flow_rows = []
    for (k, e, i, m), var in tqdm(x_E_DC.items(), desc=f"{scenario_name} flows E->DC", unit="row"):
        val = var_value(var)
        if val > 1e-8:
            flow_rows.append({
                "scenario": scenario_name,
                "flow_type": "E_to_DC",
                "product": k,
                "origin": e,
                "destination": i,
                "month": m,
                "quantity_kg": val,
                "distance_km": data["D_E_DC"].get((e, i), 0.0),
            })

    for (k, s, i, m), var in tqdm(x_Scl_DC.items(), desc=f"{scenario_name} flows Scl->DC", unit="row"):
        val = var_value(var)
        if val > 1e-8:
            flow_rows.append({
                "scenario": scenario_name,
                "flow_type": "Scl_to_DC",
                "product": k,
                "origin": s,
                "destination": i,
                "month": m,
                "quantity_kg": val,
                "distance_km": data["D_Scl_DC"].get((s, i), 0.0),
            })

    for (k, w, i, m), var in tqdm(x_WM_DC.items(), desc=f"{scenario_name} flows WM->DC", unit="row"):
        val = var_value(var)
        if val > 1e-8:
            flow_rows.append({
                "scenario": scenario_name,
                "flow_type": "WM_to_DC",
                "product": k,
                "origin": w,
                "destination": i,
                "month": m,
                "quantity_kg": val,
                "distance_km": data["D_WM_DC"].get((w, i), 0.0),
            })

    for (k, i, j, m), var in tqdm(x_DC_DS.items(), desc=f"{scenario_name} flows DC->DS", unit="row"):
        val = var_value(var)
        if val > 1e-8:
            flow_rows.append({
                "scenario": scenario_name,
                "flow_type": "DC_to_DS",
                "product": k,
                "origin": i,
                "destination": j,
                "month": m,
                "quantity_kg": val,
                "distance_km": data["D_DC_DS"].get((i, j), 0.0),
            })

    for (k, i, m), var in tqdm(x_DC_MDC.items(), desc=f"{scenario_name} flows DC->MDC", unit="row"):
        val = var_value(var)
        if val > 1e-8:
            flow_rows.append({
                "scenario": scenario_name,
                "flow_type": "DC_to_MDC",
                "product": k,
                "origin": i,
                "destination": "MDC",
                "month": m,
                "quantity_kg": val,
                "distance_km": None,
            })

    flows_df = pd.DataFrame(flow_rows)

    delivery_rows = []
    for (k, i, m), var in tqdm(x_F_C.items(), desc=f"{scenario_name} deliveries", unit="row"):
        val = var_value(var)
        if val > 1e-8:
            delivery_rows.append({
                "scenario": scenario_name,
                "product": k,
                "facility": i,
                "facility_type": data["facility_type"][i],
                "month": m,
                "quantity_kg": val,
                "last_mile_distance_km": data["D_lm"].get(i, 0.0),
                "revenue_usd": val * data["P_k"][k],
            })
    deliveries_df = pd.DataFrame(delivery_rows)

    mdc_rows = []
    for (z, i, m), var in tqdm(u_zim.items(), desc=f"{scenario_name} MDC assignment", unit="row"):
        val = var_value(var)
        if val > 0.5:
            mdc_rows.append({
                "scenario": scenario_name,
                "z_name": z,
                "start_cd": i,
                "month": m,
                "u_value": val,
                "mdc_route_distance_km": data["D_MDC"].get((z, i), 0.0),
            })
    mdc_df = pd.DataFrame(mdc_rows)

    log(
        f"Result sizes -> summary={len(summary_df)}, config={len(config_df)}, "
        f"open={len(open_facilities_df)}, purchases={len(purchases_df)}, "
        f"flows={len(flows_df)}, deliveries={len(deliveries_df)}, mdc={len(mdc_df)}"
    )

    return {
        "summary": summary_df,
        "config_selection": config_df,
        "open_facilities": open_facilities_df,
        "purchases": purchases_df,
        "flows": flows_df,
        "deliveries": deliveries_df,
        "mdc_assignment": mdc_df,
    }


# ============================================================
# SAVE RESULTS
# ============================================================
def save_results(result_tables: Dict[str, pd.DataFrame], scenario_name: str) -> None:
    section(f"SAVING RESULTS -> {scenario_name}")

    scenario_dir = OUTPUT_DIR / scenario_name
    scenario_dir.mkdir(parents=True, exist_ok=True)
    log(f"Output dir: {scenario_dir.resolve()}")

    for name, df in tqdm(result_tables.items(), desc=f"Saving CSVs {scenario_name}", unit="file"):
        out_path = scenario_dir / f"{name}.csv"
        if df is None or df.empty:
            pd.DataFrame().to_csv(out_path, index=False)
        else:
            df.to_csv(out_path, index=False)
        log(f"Saved: {out_path}")


# ============================================================
# RUN SCENARIO
# ============================================================
def run_scenario(base_data: Dict, scenario_name: str) -> pd.DataFrame:
    section(f"RUNNING SCENARIO: {scenario_name}")

    with Timer(f"Scenario total [{scenario_name}]"):
        scenario_data = filter_scenario(base_data, scenario_name)

        if len(scenario_data["Z"]) == 0:
            log(f"[WARNING] No feasible configurations found for scenario {scenario_name}")
            empty_summary = pd.DataFrame([{
                "scenario": scenario_name,
                "status": "NO_CONFIGS",
                "objective_value": None,
                "selected_z": None,
                "covered_population": None,
            }])
            save_results({"summary": empty_summary}, scenario_name)
            return empty_summary

        model_obj = build_model(scenario_data, scenario_name)
        solve_model(model_obj["model"], scenario_name)
        result_tables = extract_results(scenario_data, model_obj, scenario_name)
        save_results(result_tables, scenario_name)

        summary_df = result_tables["summary"]
        print(summary_df.to_string(index=False))
        return summary_df


# ============================================================
# MAIN
# ============================================================
def main():
    section("FRESH VEGGIE - PULP OPTIMIZATION")

    with Timer("Full pipeline"):
        raw_data = load_all_data()
        base_data = preprocess(raw_data)

        scenarios = [
            #"dc_only",
            "dc_ds",
            #"dc_mdcp",
            #"all",
        ]

        all_summaries = []
        for scenario in tqdm(scenarios, desc="Overall scenarios", unit="scenario"):
            summary_df = run_scenario(base_data, scenario)
            all_summaries.append(summary_df)

        final_summary = pd.concat(all_summaries, ignore_index=True)
        final_summary_path = OUTPUT_DIR / "summary_all_scenarios.csv"
        final_summary.to_csv(final_summary_path, index=False)

        section("FINAL SUMMARY")
        print(final_summary.to_string(index=False))
        log(f"Saved global summary to: {final_summary_path.resolve()}")
        log(f"All results saved under: {OUTPUT_DIR.resolve()}")


if __name__ == "__main__":
    main()