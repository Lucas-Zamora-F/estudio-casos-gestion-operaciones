import pandas as pd
import itertools
from math import inf


def load_distance_cd_mdcp(path: str) -> dict:
    df = pd.read_csv(path)
    df.columns = [col.strip() for col in df.columns]

    cd_name_col = df.columns[0]
    mdcp_cols = df.columns[1:]

    dist_cd_mdcp = {}

    for _, row in df.iterrows():
        cd_name = str(row[cd_name_col]).strip()
        dist_cd_mdcp[cd_name] = {}

        for mdcp in mdcp_cols:
            dist_cd_mdcp[cd_name][mdcp] = float(row[mdcp])

    return dist_cd_mdcp


def load_distance_mdcp_mdcp(path: str) -> dict:
    df = pd.read_csv(path)
    df.columns = [col.strip() for col in df.columns]

    mdcp_name_col = df.columns[0]
    mdcp_cols = df.columns[1:]

    dist_mdcp = {}

    for _, row in df.iterrows():
        mdcp_a = str(row[mdcp_name_col]).strip()
        dist_mdcp[mdcp_a] = {}

        for mdcp_b in mdcp_cols:
            dist_mdcp[mdcp_a][mdcp_b] = float(row[mdcp_b])

    return dist_mdcp


def get_active_facilities(row: pd.Series, prefix: str) -> list:
    active = []
    for col in row.index:
        if col.startswith(prefix) and int(row[col]) == 1:
            active.append(col)
    return active


def compute_route_distance(
    start_cd: str,
    mdcp_sequence: tuple,
    dist_cd_mdcp: dict,
    dist_mdcp: dict
) -> float:

    if len(mdcp_sequence) == 0:
        return 0.0

    total_distance = 0.0

    # CD -> first MDCP
    total_distance += dist_cd_mdcp[start_cd][mdcp_sequence[0]]

    # MDCP -> MDCP
    for i in range(len(mdcp_sequence) - 1):
        total_distance += dist_mdcp[mdcp_sequence[i]][mdcp_sequence[i + 1]]

    # last MDCP -> CD
    total_distance += dist_cd_mdcp[start_cd][mdcp_sequence[-1]]

    return total_distance


def build_route_string(start_cd: str, mdcp_sequence: tuple) -> str:
    """
    Builds full route string:
    CD -> MDCP1 -> MDCP2 -> ... -> CD
    """
    route = [start_cd] + list(mdcp_sequence) + [start_cd]
    return " -> ".join(route)


def find_best_route_for_z(
    active_cds: list,
    active_mdcps: list,
    dist_cd_mdcp: dict,
    dist_mdcp: dict
) -> tuple:

    best_cd = None
    best_distance = inf
    best_sequence = None

    for cd in active_cds:
        for perm in itertools.permutations(active_mdcps):
            route_distance = compute_route_distance(
                start_cd=cd,
                mdcp_sequence=perm,
                dist_cd_mdcp=dist_cd_mdcp,
                dist_mdcp=dist_mdcp
            )

            if route_distance < best_distance:
                best_distance = route_distance
                best_cd = cd
                best_sequence = perm

    return best_cd, best_distance, best_sequence


def main():
    # Paths
    z_path = r"data\SOLVER DATA\Z.csv"
    d_cd_mdcp_path = r"data\SOLVER DATA\D_CD_MDCP.csv"
    d_mdcp_path = r"data\SOLVER DATA\D_MDCP.csv"
    output_path = r"data\SOLVER DATA\g(z).csv"

    # Load data
    z_df = pd.read_csv(z_path)
    z_df.columns = [col.strip() for col in z_df.columns]

    dist_cd_mdcp = load_distance_cd_mdcp(d_cd_mdcp_path)
    dist_mdcp = load_distance_mdcp_mdcp(d_mdcp_path)

    # Detect columns
    cd_cols = [col for col in z_df.columns if col.startswith("CD_")]
    mdcp_cols = [col for col in z_df.columns if col.startswith("MDCP_")]

    if "z_name" not in z_df.columns:
        raise ValueError("Column 'z_name' not found.")
    if not cd_cols:
        raise ValueError("No CD columns found.")
    if not mdcp_cols:
        raise ValueError("No MDCP columns found.")

    # Filter valid z
    filtered_z_df = z_df[
        (z_df[cd_cols].sum(axis=1) >= 1) &
        (z_df[mdcp_cols].sum(axis=1) >= 1)
    ].copy()

    results = []

    for _, row in filtered_z_df.iterrows():
        z_name = row["z_name"]

        active_cds = get_active_facilities(row, "CD_")
        active_mdcps = get_active_facilities(row, "MDCP_")

        best_cd, min_distance, best_sequence = find_best_route_for_z(
            active_cds,
            active_mdcps,
            dist_cd_mdcp,
            dist_mdcp
        )

        route_str = build_route_string(best_cd, best_sequence)

        results.append({
            "z_name": z_name,
            "min_distance_km": min_distance,
            "start_cd": best_cd,
            "route": route_str
        })

    results_df = pd.DataFrame(results)
    results_df.to_csv(output_path, index=False)

    print(f"Original rows: {len(z_df)}")
    print(f"Processed rows: {len(filtered_z_df)}")
    print(f"Saved to: {output_path}\n")

    print(results_df.head())


if __name__ == "__main__":
    main()