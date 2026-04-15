from __future__ import annotations

import json
import shutil
import time
from pathlib import Path
from typing import Dict, List

import pandas as pd


# ============================================================
# CONFIG
# ============================================================
BASE_RESULTS_DIR = Path("main/optimization_results")
GRAPH_DIR_NAME = "graph"
PLOT_CONTEXT_FILENAME = "plot_context.json"

DEFAULT_MONTHS_ORDER = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
]

EXPECTED_CSVS = [
    "summary.csv",
    "config_selection.csv",
    "open_facilities.csv",
    "purchases.csv",
    "flows.csv",
    "deliveries.csv",
    "mdc_assignment.csv",
]


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
# HELPERS
# ============================================================
def normalize_text(x) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip()


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
    except Exception as e:
        log(f"[WARNING] No se pudo leer {path}: {e}")
        return pd.DataFrame()


def extract_months_from_df(df: pd.DataFrame) -> set[str]:
    if df.empty or "month" not in df.columns:
        return set()

    months = set()

    for value in df["month"].dropna().astype(str):
        value = value.strip()
        if value:
            months.add(value)

    return months


def detect_months_in_model_folder(model_dir: Path) -> List[str]:
    candidate_files = [
        model_dir / "deliveries.csv",
        model_dir / "flows.csv",
        model_dir / "purchases.csv",
        model_dir / "mdc_assignment.csv",
    ]

    detected_months = set()

    for csv_path in candidate_files:
        df = safe_read_csv(csv_path)
        detected_months.update(extract_months_from_df(df))

    ordered = [m for m in DEFAULT_MONTHS_ORDER if m in detected_months]

    if ordered:
        return ordered

    return DEFAULT_MONTHS_ORDER.copy()


def collect_csv_status(model_dir: Path) -> Dict[str, Dict]:
    out = {}

    for csv_name in EXPECTED_CSVS:
        csv_path = model_dir / csv_name
        exists = csv_path.exists() and csv_path.is_file()
        size_bytes = csv_path.stat().st_size if exists else 0

        df = safe_read_csv(csv_path) if exists else pd.DataFrame()

        out[csv_name] = {
            "exists": exists,
            "size_bytes": int(size_bytes),
            "rows": int(len(df)) if exists else 0,
            "columns": list(df.columns) if not df.empty else [],
        }

    return out


def load_summary_info(model_dir: Path) -> Dict:
    summary_path = model_dir / "summary.csv"
    df = safe_read_csv(summary_path)

    if df.empty:
        return {
            "scenario": model_dir.name,
            "status": None,
            "objective_value": None,
            "selected_z": None,
            "covered_population": None,
        }

    first = df.iloc[0].to_dict()

    return {
        "scenario": first.get("scenario", model_dir.name),
        "status": first.get("status"),
        "objective_value": first.get("objective_value"),
        "selected_z": first.get("selected_z"),
        "covered_population": first.get("covered_population"),
    }


def recreate_graph_folder(model_dir: Path) -> Path:
    graph_dir = model_dir / GRAPH_DIR_NAME

    if graph_dir.exists():
        log(f"    Borrando carpeta existente: {graph_dir}")
        shutil.rmtree(graph_dir)

    graph_dir.mkdir(parents=True, exist_ok=True)
    log(f"    [OK] Carpeta creada: {graph_dir}")

    return graph_dir


def create_month_folders(graph_dir: Path, months: List[str]) -> List[str]:
    created = []

    for month in months:
        month_dir = graph_dir / month
        month_dir.mkdir(parents=True, exist_ok=True)
        created.append(str(month_dir))
        log(f"    [OK] Carpeta creada: {month_dir}")

    return created


def build_plot_context(model_dir: Path, graph_dir: Path, months: List[str]) -> Dict:
    csv_status = collect_csv_status(model_dir)
    summary_info = load_summary_info(model_dir)

    context = {
        "model_name": model_dir.name,
        "model_dir": str(model_dir.resolve()),
        "graph_dir": str(graph_dir.resolve()),
        "months": months,
        "summary": summary_info,
        "csvs": {
            csv_name: {
                "path": str((model_dir / csv_name).resolve()),
                **csv_status[csv_name],
            }
            for csv_name in EXPECTED_CSVS
        },
    }

    return context


def save_plot_context(graph_dir: Path, context: Dict) -> Path:
    out_path = graph_dir / PLOT_CONTEXT_FILENAME

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(context, f, ensure_ascii=False, indent=2)

    log(f"    [OK] Contexto guardado: {out_path}")
    return out_path


def process_model_folder(model_dir: Path) -> Dict:
    log(f"[PROCESS] {model_dir.name}")

    graph_dir = recreate_graph_folder(model_dir)
    months = detect_months_in_model_folder(model_dir)
    create_month_folders(graph_dir, months)

    context = build_plot_context(
        model_dir=model_dir,
        graph_dir=graph_dir,
        months=months,
    )

    save_plot_context(graph_dir, context)

    log(
        f"    Resumen -> months={len(months)} | "
        f"status={context['summary']['status']} | "
        f"selected_z={context['summary']['selected_z']}"
    )

    return context


# ============================================================
# PUBLIC ENTRYPOINT
# ============================================================
def run_prepare_plot_environment(base_results_dir: Path = BASE_RESULTS_DIR) -> List[Dict]:
    section("PREPARE PLOT ENVIRONMENT")

    if not base_results_dir.exists():
        raise FileNotFoundError(f"No existe la carpeta base: {base_results_dir.resolve()}")

    model_dirs = sorted([p for p in base_results_dir.iterdir() if p.is_dir()])

    if not model_dirs:
        log("[WARNING] No se encontraron carpetas de modelos dentro de optimization_results")
        return []

    all_contexts = []

    for model_dir in model_dirs:
        context = process_model_folder(model_dir)
        all_contexts.append(context)

    section("PREPARE COMPLETE")
    log(f"Modelos procesados: {len(all_contexts)}")

    return all_contexts


# ============================================================
# MAIN
# ============================================================
def main() -> None:
    run_prepare_plot_environment()


if __name__ == "__main__":
    main()