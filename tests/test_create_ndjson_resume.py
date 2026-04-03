import importlib.util
import json
import sys
from pathlib import Path

import pytest


def _load_create_ndjson_module():
    script_path = Path(__file__).resolve().parents[1] / "create_ndjson_from_api.py"
    spec = importlib.util.spec_from_file_location("create_ndjson_from_api", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class _DummyClient:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_export_ndjson_resumes_from_saved_next_url(tmp_path, monkeypatch):
    m = _load_create_ndjson_module()

    monkeypatch.setattr(m, "_get_basic_auth_from_env", lambda: ("user", "pass"))
    monkeypatch.setattr(m.httpx, "Client", lambda **kwargs: _DummyClient())

    first_page_next = "https://example.test/fhir/Practitioner?_getpages=abc&_getpagesoffset=1000"
    calls = {"count": 0}

    def fake_fetch_resources(**kwargs):
        calls["count"] += 1
        state = m._load_or_init_resource_state(
            output_dir=kwargs["output_dir"],
            resource_type=kwargs["resource_type"],
            count=kwargs["count"],
        )
        if calls["count"] == 1:
            assert state.pages_completed == 0
            assert state.request_url == "Practitioner"
            yield m.DownloadPage(
                page_num=1,
                resources=[
                    {"resourceType": "Practitioner", "id": "p1"},
                    {"resourceType": "Practitioner", "id": "p2"},
                ],
                next_url=first_page_next,
                total_hint=3,
            )
            raise KeyboardInterrupt()

        assert state.pages_completed == 1
        assert state.request_url == first_page_next
        assert state.resources_written == 2
        yield m.DownloadPage(
            page_num=2,
            resources=[{"resourceType": "Practitioner", "id": "p3"}],
            next_url=None,
            total_hint=3,
        )

    monkeypatch.setattr(m, "fetch_resources", fake_fetch_resources)

    with pytest.raises(KeyboardInterrupt):
        m.export_ndjson(
            fhir_base_url="https://example.test/fhir/",
            output_dir=tmp_path,
            resource_types=["Practitioner"],
            count=1000,
            hard_limit=None,
            retry=m.RetryConfig(),
            log_dir=tmp_path / "log",
            url_print=m.UrlPrintConfig(print_urls=False),
            curl_debug=m.CurlDebugConfig(curl_on_error=False),
            progress_every=1000,
        )

    partial_output = (tmp_path / "practitioner.ndjson").read_text(encoding="utf-8").splitlines()
    assert [json.loads(line)["id"] for line in partial_output] == ["p1", "p2"]

    partial_state = m._load_or_init_resource_state(output_dir=tmp_path, resource_type="Practitioner", count=1000)
    assert partial_state.status == "in_progress"
    assert partial_state.pages_completed == 1
    assert partial_state.request_url == first_page_next

    m.export_ndjson(
        fhir_base_url="https://example.test/fhir/",
        output_dir=tmp_path,
        resource_types=["Practitioner"],
        count=1000,
        hard_limit=None,
        retry=m.RetryConfig(),
        log_dir=tmp_path / "log",
        url_print=m.UrlPrintConfig(print_urls=False),
        curl_debug=m.CurlDebugConfig(curl_on_error=False),
        progress_every=1000,
    )

    final_output = (tmp_path / "practitioner.ndjson").read_text(encoding="utf-8").splitlines()
    assert [json.loads(line)["id"] for line in final_output] == ["p1", "p2", "p3"]

    final_state = m._load_or_init_resource_state(output_dir=tmp_path, resource_type="Practitioner", count=1000)
    assert final_state.status == "completed"
    assert final_state.pages_completed == 2
    assert final_state.resources_written == 3


def test_sync_output_from_pages_repairs_partial_output_file(tmp_path):
    m = _load_create_ndjson_module()

    state = m._load_or_init_resource_state(output_dir=tmp_path, resource_type="Practitioner", count=1000)
    page_path = m._resource_page_path(output_dir=tmp_path, resource_type="Practitioner", page_num=1)
    page_path.parent.mkdir(parents=True, exist_ok=True)
    page_contents = '{"resourceType":"Practitioner","id":"p1"}\n'
    page_path.write_text(page_contents, encoding="utf-8")

    output_path = tmp_path / "practitioner.ndjson"
    output_path.write_text("partial", encoding="utf-8")

    state.pages_completed = 1
    state.assembled_pages = 0
    state.assembled_bytes = 0

    repaired = m._sync_output_from_pages(output_dir=tmp_path, state=state)

    assert output_path.read_text(encoding="utf-8") == page_contents
    assert repaired.assembled_pages == 1
    assert repaired.assembled_bytes == len(page_contents.encode("utf-8"))
