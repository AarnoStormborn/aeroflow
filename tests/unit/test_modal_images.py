"""Guard against Modal image / service dependency drift.

Each service declares its runtime dependencies in ``<service>/pyproject.toml``,
but Modal images in ``modal_app.py`` hand-duplicate that list via
``pip_install(...)``. When the two drift, the failure only shows up at runtime
inside a deployed container -- e.g. a missing ``xgboost`` in ``training_image``
broke the scheduled retrain with ``ModuleNotFoundError``.

These tests statically parse ``modal_app.py`` and each service's runtime source
imports, then assert every third-party import is satisfiable by the image that
runs that service.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
MODAL_APP = REPO_ROOT / "modal_app.py"

# Third-party import name -> distribution name used in pip_install().
MODULE_TO_PACKAGE = {
    "boto3": "boto3",
    "dotenv": "python-dotenv",
    "fastapi": "fastapi",
    "httpx": "httpx",
    "loguru": "loguru",
    "matplotlib": "matplotlib",
    "mlflow": "mlflow",
    "numpy": "numpy",
    "pandas": "pandas",
    "polars": "polars",
    "pyarrow": "pyarrow",
    "pydantic": "pydantic",
    "pydantic_settings": "pydantic-settings",
    "reportlab": "reportlab",
    "seaborn": "seaborn",
    "sklearn": "scikit-learn",
    "uvicorn": "uvicorn",
    "xgboost": "xgboost",
    "yaml": "pyyaml",
}

# First-party / intra-repo top-level modules that are not pip packages.
FIRST_PARTY = {"src", "tests", "dashboard", "features", "forecasting", "training", "ingestion"}

# Packages always pulled in transitively by the heavy direct deps above
# (scikit-learn, xgboost, matplotlib, seaborn, mlflow all depend on numpy;
# mlflow/pandas stack pulls pyarrow). Requiring an explicit pin for these
# would be noise, not signal. Anything NOT listed here must be declared.
TRANSITIVE_PACKAGES = {
    "numpy": "transitive dep of scikit-learn / xgboost / matplotlib / seaborn / mlflow",
    "pyarrow": "transitive dep of the pandas/mlflow stack",
}

# What each Modal image must be able to import, and which source to scan.
# `roots` are runtime code paths relative to REPO_ROOT (excludes notebooks,
# reference/sample scripts, and tests).
SERVICE_IMAGES = {
    "training_image": {
        "roots": ["model-training/src/training", "model-training/main.py"],
    },
    "forecast_image": {
        "roots": ["forecasting/src/forecasting"],
    },
    "feature_image": {
        # train_sample/ is a reference script, not part of the scheduled job.
        "roots": [
            "feature-engineering/src/features",
            "feature-engineering/src/pipeline",
            "feature-engineering/main.py",
        ],
    },
    "dashboard_image": {
        "roots": ["dashboard/src/dashboard"],
    },
}


def _pip_packages_for(image_name: str) -> set[str]:
    """Return normalized distribution names passed to pip_install for an image."""
    tree = ast.parse(MODAL_APP.read_text())
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        target = node.targets[0]
        if not (isinstance(target, ast.Name) and target.id == image_name):
            continue
        packages: set[str] = set()
        for call in ast.walk(node.value):
            if not isinstance(call, ast.Call):
                continue
            func = call.func
            if isinstance(func, ast.Attribute) and func.attr == "pip_install":
                for arg in call.args:
                    if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                        packages.add(_normalize(arg.value))
        return packages
    raise AssertionError(f"image {image_name!r} not found in {MODAL_APP.name}")


def _normalize(spec: str) -> str:
    """'scikit-learn>=1.4.0' / 'mlflow==3.15.2' -> 'scikit-learn' (lowercased)."""
    for sep in (">=", "<=", "==", "~=", "!=", ">", "<", "["):
        spec = spec.split(sep, 1)[0]
    return spec.strip().lower()


_IMPORT_ERRORS = {"ImportError", "ModuleNotFoundError", "Exception", "BaseException"}


def _optional_import_nodes(tree: ast.AST) -> set[int]:
    """id()s of Import/ImportFrom nodes guarded by `try: ... except ImportError`."""
    optional: set[int] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Try):
            continue
        guards = any(
            handler.type is None or any(getattr(elt, "id", None) in _IMPORT_ERRORS for elt in ast.walk(handler.type))
            for handler in node.handlers
        )
        if not guards:
            continue
        for inner in ast.walk(ast.Module(body=node.body, type_ignores=[])):
            if isinstance(inner, (ast.Import, ast.ImportFrom)):
                optional.add(id(inner))
    return optional


def _runtime_imports(root: Path) -> set[str]:
    """Collect required top-level third-party imports under `root`.

    Imports inside a `try/except ImportError` block are treated as optional,
    since the code explicitly tolerates their absence.
    """
    files: list[Path] = []
    if root.is_file():
        files = [root]
    elif root.is_dir():
        files = [p for p in root.rglob("*.py") if "__pycache__" not in p.parts]

    modules: set[str] = set()
    for path in files:
        try:
            tree = ast.parse(path.read_text())
        except SyntaxError:  # pragma: no cover - source should always parse
            continue
        optional = _optional_import_nodes(tree)
        for node in ast.walk(tree):
            if id(node) in optional:
                continue
            if isinstance(node, ast.Import):
                for alias in node.names:
                    modules.add(alias.name.split(".")[0])
            elif isinstance(node, ast.ImportFrom):
                if node.level:  # relative import -> first party
                    continue
                if node.module:
                    modules.add(node.module.split(".")[0])
    return modules


def _missing_packages(image_name: str) -> list[str]:
    provided = _pip_packages_for(image_name)
    missing: list[str] = []
    for rel in SERVICE_IMAGES[image_name]["roots"]:
        for module in sorted(_runtime_imports(REPO_ROOT / rel)):
            if module in sys.stdlib_module_names or module in FIRST_PARTY:
                continue
            package = MODULE_TO_PACKAGE.get(module)
            if package is None:
                pytest.fail(
                    f"{image_name}: runtime import '{module}' (in {rel}) is not in "
                    f"MODULE_TO_PACKAGE; add its pip distribution name to the mapping "
                    f"and to the image if it is third-party."
                )
            if package in provided or package in TRANSITIVE_PACKAGES:
                continue
            missing.append(f"{module} (needs {package}) in {rel}")
    return missing


@pytest.mark.parametrize("image_name", sorted(SERVICE_IMAGES))
def test_image_provides_every_runtime_import(image_name: str) -> None:
    missing = _missing_packages(image_name)
    assert not missing, f"{image_name} is missing packages required by its service code: " + "; ".join(missing)


def test_training_image_installs_xgboost() -> None:
    """Regression guard for the scheduled-retrain ModuleNotFoundError."""
    assert "xgboost" in _pip_packages_for("training_image")
