import importlib.util
from pathlib import Path


def _load_download_cms_ndjson_module():
    script_path = Path(__file__).resolve().parents[1] / "download_cms_ndjson.py"
    spec = importlib.util.spec_from_file_location("download_cms_ndjson", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_parse_resource_types_default():
    m = _load_download_cms_ndjson_module()

    got = m._parse_resource_types(None)
    assert got == list(m.DEFAULT_RESOURCE_TYPES)


def test_parse_resource_types_custom_list():
    m = _load_download_cms_ndjson_module()

    got = m._parse_resource_types("Practitioner, Organization ,Location")
    assert got == ["Practitioner", "Organization", "Location"]


def test_build_create_ndjson_cmd_single_resource_type():
    m = _load_download_cms_ndjson_module()

    cmd = m._build_create_ndjson_cmd(
        fhir_url="https://example.test/fhir/",
        output_dir=Path("/tmp/out"),
        count=123,
        stop_after_this_many=456,
        resource_type="Practitioner",
    )

    # Script path is element 2; element 0 is the python executable and element 1 is -u.
    assert cmd[2].endswith("create_ndjson_from_api.py")
    assert "--count" in cmd
    assert cmd[cmd.index("--count") + 1] == "123"
    assert "--resource-types" in cmd
    assert cmd[cmd.index("--resource-types") + 1] == "Practitioner"
    assert "--stop-after-this-many" in cmd
    assert cmd[cmd.index("--stop-after-this-many") + 1] == "456"


def test_max_redownload_attempts_default_is_zero():
    m = _load_download_cms_ndjson_module()

    p = m.build_arg_parser()
    args = p.parse_args(["/tmp/out"])

    assert args.max_redownload_attempts == 0


def test_resource_has_in_progress_state(tmp_path):
    m = _load_download_cms_ndjson_module()

    # No state file.
    assert m._resource_has_in_progress_state(output_dir=tmp_path, resource_type="Practitioner") is False

    # Create a fake state.json.
    state_dir = tmp_path / "download_state" / "Practitioner"
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "state.json").write_text('{"status": "in_progress"}\n', encoding="utf-8")

    assert m._resource_has_in_progress_state(output_dir=tmp_path, resource_type="Practitioner") is True

    # Completed should not count.
    (state_dir / "state.json").write_text('{"status": "completed"}\n', encoding="utf-8")
    assert m._resource_has_in_progress_state(output_dir=tmp_path, resource_type="Practitioner") is False
