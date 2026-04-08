from itertools import product
from pathlib import Path
import csv
import pandas as pd


def load_names(path):
    df = pd.read_csv(path)
    return df.iloc[:, 0].dropna().astype(str).tolist()


def main():
    base_path = Path("data") / "SOLVER DATA"

    # Load names
    cd_names = load_names(base_path / "CD.csv")
    ds_names = load_names(base_path / "DS.csv")
    mdcp_names = load_names(base_path / "MDCP.csv")

    all_names = cd_names + ds_names + mdcp_names
    n = len(all_names)

    print(f"Total facilities: {n}")
    print(f"Total combinations: {2**n:,}")

    output_path = base_path / "Z.csv"

    # Create file
    with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)

        # Header
        writer.writerow(["z_name"] + all_names)

        # Generate combinations
        for i, comb in enumerate(product([0, 1], repeat=n), start=1):
            z_name = f"z{i:06d}"
            writer.writerow([z_name, *comb])

    print(f"[OK] Z.csv generated at: {output_path}")


if __name__ == "__main__":
    main()