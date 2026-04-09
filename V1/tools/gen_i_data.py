import pandas as pd
from pathlib import Path


def main():
    base_path = Path("data") / "SOLVER DATA"

    z_path = base_path / "Z.csv"
    output_path = base_path / "I.csv"

    # Load Z.csv
    df = pd.read_csv(z_path)

    # Facility columns (all except z_name)
    facility_cols = df.columns[1:]

    # Filter singletons (only one active facility)
    df_singletons = df[df[facility_cols].sum(axis=1) == 1].copy()

    # Save result
    df_singletons.to_csv(output_path, index=False, encoding="utf-8-sig")

    print(f"[OK] I.csv generated at: {output_path}")
    print(f"Total singletons: {len(df_singletons)}")


if __name__ == "__main__":
    main()