import importlib.util
import sys
from pathlib import Path


def _load_create_ndjson_from_api_module():
    script_path = Path(__file__).resolve().parents[1] / "create_ndjson_from_api.py"

    # Ensure repo root is importable so `create_ndjson_from_api.py` can import
    # sibling modules like `check_dependencies.py`.
    repo_root = str(Path(__file__).resolve().parents[1])
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)

    module_name = "create_ndjson_from_api"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # Dataclasses may look up the module by name during class creation; ensure
    # it's present in sys.modules while executing.
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_normalize_timeout_max_defaults_to_initial_timeout():
    m = _load_create_ndjson_from_api_module()

    assert m._normalize_timeout_max(timeout_seconds=120.0, timeout_max_seconds=None) == 120.0


def test_normalize_timeout_max_never_below_initial_timeout():
    m = _load_create_ndjson_from_api_module()

    assert m._normalize_timeout_max(timeout_seconds=120.0, timeout_max_seconds=60.0) == 120.0


def test_normalize_timeout_max_allows_increase():
    m = _load_create_ndjson_from_api_module()

    assert m._normalize_timeout_max(timeout_seconds=120.0, timeout_max_seconds=300.0) == 300.0
