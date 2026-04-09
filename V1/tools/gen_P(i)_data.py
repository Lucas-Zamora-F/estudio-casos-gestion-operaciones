import pandas as pd
from pathlib import Path


def main():
    base_path = Path("data") / "SOLVER DATA"

    i_path = base_path / "I.csv"
    pz_path = base_path / "p(z).csv"
    output_path = base_path / "P(I).csv"

    # Load data
    df_i = pd.read_csv(i_path)
    df_pz = pd.read_csv(pz_path)

    # Extract singleton z names
    z_names = df_i["z_name"]

    # Filter p(z) using z
    df_pi = df_pz[df_pz["z"].isin(z_names)].copy()

    # Save result
    df_pi.to_csv(output_path, index=False, encoding="utf-8-sig")

    print(f"[OK] P(I).csv generated at: {output_path}")
    print(f"Total rows: {len(df_pi)}")


if __name__ == "__main__":
    main()
