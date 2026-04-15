from __future__ import annotations

import importlib.util
import sys
import time
from pathlib import Path
from types import ModuleType
from typing import Any, Dict, List


# ============================================================
# CONFIG
# ============================================================
SCRIPT_DIR = Path(__file__).resolve().parent
PLOTTER_STUFF_DIR = SCRIPT_DIR / "plotter_stuff"

PREPARE_SCRIPT = PLOTTER_STUFF_DIR / "00_prepare_plot_environment.py"
BUILD_PLOT_DATA_SCRIPT = PLOTTER_STUFF_DIR / "01_build_plot_data.py"
PLOT_SUPPLIER_TO_DC_SCRIPT = PLOTTER_STUFF_DIR / "10_plot_supplier_to_dc_routes.py"


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
# DYNAMIC MODULE LOADING
# ============================================================
def load_module_from_path(module_name: str, file_path: Path) -> ModuleType:
    if not file_path.exists():
        raise FileNotFoundError(f"No existe el script: {file_path.resolve()}")

    spec = importlib.util.spec_from_file_location(module_name, str(file_path))
    if spec is None or spec.loader is None:
        raise ImportError(f"No se pudo construir spec para: {file_path.resolve()}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def load_module(module_name: str, file_path: Path) -> ModuleType:
    log(f"Cargando script: {file_path.name}")
    return load_module_from_path(module_name=module_name, file_path=file_path)


def get_required_function(module: ModuleType, function_name: str):
    if not hasattr(module, function_name):
        raise AttributeError(
            f"El script {Path(module.__file__).name} no contiene la función esperada: {function_name}"
        )
    return getattr(module, function_name)


def get_optional_function(module: ModuleType, function_names: list[str]):
    for function_name in function_names:
        if hasattr(module, function_name):
            return getattr(module, function_name)
    return None


# ============================================================
# PIPELINE HELPERS
# ============================================================
def normalize_model_name(name: Any) -> str:
    return str(name).strip()


def index_prepare_contexts_by_model(
    prepare_contexts: List[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    indexed: Dict[str, Dict[str, Any]] = {}

    for ctx in prepare_contexts:
        model_name = normalize_model_name(ctx.get("model_name", ""))
        if not model_name:
            continue
        indexed[model_name] = ctx

    return indexed


def ensure_plot_data_structure(plot_data: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(plot_data, dict):
        raise TypeError("plot_data debe ser un dict.")

    if "models" not in plot_data or not isinstance(plot_data["models"], dict):
        raise KeyError("plot_data no contiene una clave válida 'models'.")

    if "global" not in plot_data or not isinstance(plot_data["global"], dict):
        plot_data["global"] = {}

    return plot_data


def ensure_model_defaults(model_name: str, model_data: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(model_data, dict):
        raise TypeError(f"model_data para {model_name} debe ser dict.")

    model_data.setdefault("model_name", model_name)
    model_data.setdefault("months", [])
    model_data.setdefault("supplier_to_dc", {})

    supplier_to_dc = model_data["supplier_to_dc"]
    if not isinstance(supplier_to_dc, dict):
        raise TypeError(f"model_data['supplier_to_dc'] para {model_name} debe ser dict.")

    supplier_to_dc.setdefault("connections", None)
    supplier_to_dc.setdefault("summary", None)

    return model_data


def merge_single_model_with_prepare_context(
    model_name: str,
    model_data: Dict[str, Any],
    prepare_ctx: Dict[str, Any],
) -> Dict[str, Any]:
    model_data = ensure_model_defaults(model_name, model_data)

    # preserve what 01 already produced and only complement missing context
    model_data["model_name"] = model_name
    model_data["model_dir"] = str(prepare_ctx.get("model_dir", model_data.get("model_dir", "")))
    model_data["graph_dir"] = str(prepare_ctx.get("graph_dir", model_data.get("graph_dir", "")))

    prepare_months = prepare_ctx.get("months", [])
    current_months = model_data.get("months", [])
    if prepare_months:
        model_data["months"] = prepare_months
    elif current_months:
        model_data["months"] = current_months

    # keep raw contextual info available for any downstream plot script
    model_data["prepare_summary"] = prepare_ctx.get("summary")
    model_data["csvs"] = prepare_ctx.get("csvs", {})
    model_data["plot_context"] = prepare_ctx

    return model_data


def enrich_plot_data_with_prepare_context(
    plot_data: Dict[str, Any],
    prepare_contexts: List[Dict[str, Any]],
) -> Dict[str, Any]:
    plot_data = ensure_plot_data_structure(plot_data)

    prepare_by_model = index_prepare_contexts_by_model(prepare_contexts)

    for model_name, model_data in list(plot_data["models"].items()):
        clean_name = normalize_model_name(model_name)

        if clean_name in prepare_by_model:
            plot_data["models"][clean_name] = merge_single_model_with_prepare_context(
                model_name=clean_name,
                model_data=model_data,
                prepare_ctx=prepare_by_model[clean_name],
            )
        else:
            # If 01 found a model but 00 did not, keep it alive with defaults.
            plot_data["models"][clean_name] = ensure_model_defaults(clean_name, model_data)
            log(f"[WARNING] {clean_name}: no apareció en prepare_contexts. Se conserva igual.")

        if clean_name != model_name:
            del plot_data["models"][model_name]

    plot_data["global"]["prepare_contexts_by_model"] = prepare_by_model
    plot_data["global"]["prepare_contexts"] = prepare_contexts

    return plot_data


def validate_pipeline_inputs(
    prepare_contexts: List[Dict[str, Any]],
    plot_data: Dict[str, Any],
) -> None:
    if not isinstance(prepare_contexts, list):
        raise TypeError("prepare_contexts debe ser una lista.")

    if not isinstance(plot_data, dict):
        raise TypeError("plot_data debe ser un dict.")

    if "models" not in plot_data:
        raise KeyError("plot_data no contiene la clave 'models'.")

    if not isinstance(plot_data["models"], dict):
        raise TypeError("plot_data['models'] debe ser dict.")

    if not prepare_contexts:
        log("[WARNING] prepare_contexts está vacío.")

    if not plot_data["models"]:
        log("[WARNING] plot_data['models'] está vacío.")


def print_plot_data_overview(plot_data: Dict[str, Any]) -> None:
    log(f"Modelos listos para plotting: {len(plot_data['models'])}")

    for model_name, model_data in plot_data["models"].items():
        supplier_to_dc = model_data.get("supplier_to_dc", {})
        connections = supplier_to_dc.get("connections")
        summary_df = supplier_to_dc.get("summary")

        n_connections = len(connections) if hasattr(connections, "__len__") and connections is not None else 0
        n_summary = len(summary_df) if hasattr(summary_df, "__len__") and summary_df is not None else 0

        log(
            f"    {model_name:<25} | "
            f"months={len(model_data.get('months', [])):<2} | "
            f"connections={n_connections:<5} | "
            f"summary={n_summary:<5} | "
            f"graph_dir={model_data.get('graph_dir')}"
        )


# ============================================================
# STEP RUNNERS
# ============================================================
def run_prepare_step() -> List[Dict[str, Any]]:
    module = load_module(
        module_name="plotter_prepare_environment",
        file_path=PREPARE_SCRIPT,
    )

    fn = get_required_function(module, "run_prepare_plot_environment")
    result = fn()

    if result is None:
        raise RuntimeError("00_prepare_plot_environment.py devolvió None.")

    if not isinstance(result, list):
        raise TypeError("00_prepare_plot_environment.py debe devolver una lista de contextos.")

    return result


def run_build_plot_data_step() -> Dict[str, Any]:
    module = load_module(
        module_name="plotter_build_plot_data",
        file_path=BUILD_PLOT_DATA_SCRIPT,
    )

    fn = get_optional_function(
        module,
        [
            "run_build_plot_data",
            "build_plot_data",
        ],
    )

    if fn is None:
        raise AttributeError(
            "01_build_plot_data.py debe contener 'run_build_plot_data' o 'build_plot_data'."
        )

    result = fn()

    if result is None:
        raise RuntimeError("01_build_plot_data.py devolvió None.")

    if not isinstance(result, dict):
        raise TypeError("01_build_plot_data.py debe devolver un dict plot_data.")

    return ensure_plot_data_structure(result)


def run_plot_supplier_to_dc_step(plot_data: Dict[str, Any]) -> None:
    module = load_module(
        module_name="plotter_supplier_to_dc_routes",
        file_path=PLOT_SUPPLIER_TO_DC_SCRIPT,
    )

    fn = get_required_function(module, "run_plot_supplier_to_dc_routes")
    fn(plot_data=plot_data)


# ============================================================
# MAIN PIPELINE
# ============================================================
def run_plotter_pipeline() -> Dict[str, Any]:
    section("PLOTTER ORCHESTRATOR")

    log(f"Script dir        : {SCRIPT_DIR}")
    log(f"Plotter stuff dir : {PLOTTER_STUFF_DIR}")

    section("STEP 1 - PREPARE PLOT ENVIRONMENT")
    prepare_contexts = run_prepare_step()

    section("STEP 2 - BUILD PLOT DATA")
    plot_data = run_build_plot_data_step()

    validate_pipeline_inputs(
        prepare_contexts=prepare_contexts,
        plot_data=plot_data,
    )

    section("STEP 3 - MERGE PREPARE CONTEXT WITH PLOT DATA")
    plot_data = enrich_plot_data_with_prepare_context(
        plot_data=plot_data,
        prepare_contexts=prepare_contexts,
    )

    print_plot_data_overview(plot_data)

    section("STEP 4 - PLOT SUPPLIER TO DC ROUTES")
    run_plot_supplier_to_dc_step(plot_data=plot_data)

    section("PLOTTER PIPELINE COMPLETE")
    return plot_data


# ============================================================
# MAIN
# ============================================================
def main() -> None:
    run_plotter_pipeline()


if __name__ == "__main__":
    main()