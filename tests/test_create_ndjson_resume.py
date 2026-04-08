import importlib.util
import json
import sys
from pathlib import Path

import httpx
import pytest


def _load_create_ndjson_module():
    script_path = Path(__file__).resolve().parents[1] / "create_ndjson_from_api.py"

    # Ensure repo root is importable so `create_ndjson_from_api.py` can import
    # sibling modules like `check_dependencies.py`.
    repo_root = str(Path(__file__).resolve().parents[1])
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)

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


def test_fetch_resources_uses_saved_next_url_after_page_commit(tmp_path, monkeypatch):
    m = _load_create_ndjson_module()

    class _FakeResponse:
        def __init__(self, payload):
            self._payload = payload
            self.status_code = 200
            self.headers = {"Content-Type": "application/fhir+json"}

        def json(self):
            return self._payload

    request_urls: list[str] = []

    def fake_request_with_retry(client, method, url, *, params, retry, log_fn, context, curl_debug, curl_auth):
        request_urls.append(str(url))
        if len(request_urls) == 1:
            return _FakeResponse(
                {
                    "resourceType": "Bundle",
                    "entry": [{"resource": {"resourceType": "Practitioner", "id": "p1"}}],
                    "link": [{"relation": "next", "url": "https://example.test/fhir/Practitioner?_getpages=abc&_getpagesoffset=1000"}],
                }
            )
        if len(request_urls) == 2:
            assert str(url) == "https://example.test/fhir/Practitioner?_getpages=abc&_getpagesoffset=1000"
            return _FakeResponse(
                {
                    "resourceType": "Bundle",
                    "entry": [{"resource": {"resourceType": "Practitioner", "id": "p2"}}],
                }
            )
        raise AssertionError("unexpected extra request")

    monkeypatch.setattr(m, "_request_with_retry", fake_request_with_retry)

    state = m._load_or_init_resource_state(output_dir=tmp_path, resource_type="Practitioner", count=1000)
    pages = m.fetch_resources(
        client=object(),
        output_dir=tmp_path,
        resource_type="Practitioner",
        count=1000,
        hard_limit=None,
        retry=m.RetryConfig(),
        log_dir=tmp_path / "log",
        url_print=m.UrlPrintConfig(print_urls=False),
        curl_debug=m.CurlDebugConfig(curl_on_error=False),
        curl_auth=None,
    )

    first_page = next(pages)
    state = m._commit_download_page(
        output_dir=tmp_path,
        state=state,
        page=first_page,
        hard_limit_reached=False,
    )

    second_page = next(pages)
    assert second_page.resources[0]["id"] == "p2"
    assert request_urls == [
        "Practitioner",
        "https://example.test/fhir/Practitioner?_getpages=abc&_getpagesoffset=1000",
    ]


def test_fetch_resources_shrinks_count_when_remaining_known(tmp_path, monkeypatch):
    m = _load_create_ndjson_module()

    class _FakeResponse:
        def __init__(self, payload):
            self._payload = payload
            self.status_code = 200
            self.headers = {"Content-Type": "application/fhir+json"}

        def json(self):
            return self._payload

    # Seed state: we already wrote 1950 resources, and total is 2000 => remaining 50.
    state = m._load_or_init_resource_state(output_dir=tmp_path, resource_type="Practitioner", count=1000)
    state.resources_written = 1950
    state.request_params = {"_count": "1000", "page_size": "1000", "_total": "none"}
    state.total_hint = 2000
    m._save_resource_state(output_dir=tmp_path, state=state)

    captured: dict[str, dict[str, str] | None] = {"params": None}

    def fake_request_with_retry(client, method, url, *, params, retry, log_fn, context, curl_debug, curl_auth):
        captured["params"] = dict(params) if params is not None else None
        return _FakeResponse(
            {
                "resourceType": "Bundle",
                "total": 2000,
                "entry": [{"resource": {"resourceType": "Practitioner", "id": "p_last"}}],
            }
        )

    monkeypatch.setattr(m, "_request_with_retry", fake_request_with_retry)

    pages = m.fetch_resources(
        client=object(),
        output_dir=tmp_path,
        resource_type="Practitioner",
        count=1000,
        hard_limit=None,
        retry=m.RetryConfig(),
        log_dir=tmp_path / "log",
        url_print=m.UrlPrintConfig(print_urls=False),
        curl_debug=m.CurlDebugConfig(curl_on_error=False),
        curl_auth=None,
    )
    _ = next(pages)

    assert captured["params"] is not None
    # We try remaining+1 first to hedge against server off-by-one total bugs.
    assert captured["params"]["_count"] == "51"
    assert captured["params"]["page_size"] == "51"


def test_fetch_resources_last_page_off_by_one_fallback(tmp_path, monkeypatch):
    m = _load_create_ndjson_module()

    class _FakeResponse:
        def __init__(self, payload):
            self._payload = payload
            self.status_code = 200
            self.headers = {"Content-Type": "application/fhir+json"}

        def json(self):
            return self._payload

    # Seed state: remaining is 50.
    state = m._load_or_init_resource_state(output_dir=tmp_path, resource_type="Practitioner", count=1000)
    state.resources_written = 1950
    state.request_params = {"_count": "1000", "page_size": "1000", "_total": "none"}
    state.total_hint = 2000
    m._save_resource_state(output_dir=tmp_path, state=state)

    tried_counts: list[str] = []

    def fake_request_with_retry(client, method, url, *, params, retry, log_fn, context, curl_debug, curl_auth):
        assert params is not None
        tried_counts.append(str(params.get("_count")))
        if params.get("_count") == "51":
            # Simulate buggy server rejecting the first off-by-one guess.
            raise httpx.HTTPStatusError(
                "bad request",
                request=httpx.Request("GET", "https://example.test"),
                response=httpx.Response(400, json={"resourceType": "OperationOutcome"}),
            )
        return _FakeResponse(
            {
                "resourceType": "Bundle",
                "total": 2000,
                "entry": [{"resource": {"resourceType": "Practitioner", "id": "p_last"}}],
            }
        )

    monkeypatch.setattr(m, "_request_with_retry", fake_request_with_retry)

    pages = m.fetch_resources(
        client=object(),
        output_dir=tmp_path,
        resource_type="Practitioner",
        count=1000,
        hard_limit=None,
        retry=m.RetryConfig(),
        log_dir=tmp_path / "log",
        url_print=m.UrlPrintConfig(print_urls=False),
        curl_debug=m.CurlDebugConfig(curl_on_error=False),
        curl_auth=None,
    )
    _ = next(pages)

    # We should have tried 51 first (with page_size), retried 51 without
    # page_size (parameter conflict handling), then fallen back to 50.
    assert tried_counts[:3] == ["51", "51", "50"]


def test_fetch_resources_stops_if_server_has_more_than_expected_total_plus_overrun(tmp_path, monkeypatch, capsys):
    m = _load_create_ndjson_module()

    # Make the threshold small for the test.
    monkeypatch.setattr(m, "SERVER_ESTIMATE_MAX_OVERRUN", 2)

    # Seed state such that expected_total is known (from count endpoint).
    state = m._load_or_init_resource_state(output_dir=tmp_path, resource_type="Practitioner", count=1000)
    state.expected_total = 10
    state.total_hint = 10
    state.resources_written = 12  # already 2 over expected
    m._save_resource_state(output_dir=tmp_path, state=state)

    class _FakeResponse:
        def __init__(self, payload):
            self._payload = payload
            self.status_code = 200
            self.headers = {"Content-Type": "application/fhir+json"}

        def json(self):
            return self._payload

    def fake_request_with_retry(client, method, url, *, params, retry, log_fn, context, curl_debug, curl_auth):
        return _FakeResponse(
            {
                "resourceType": "Bundle",
                "entry": [
                    {"resource": {"resourceType": "Practitioner", "id": "p1"}},
                    {"resource": {"resourceType": "Practitioner", "id": "p2"}},
                    {"resource": {"resourceType": "Practitioner", "id": "p3"}},
                ],
                "link": [{"relation": "next", "url": "https://example.test/fhir/Practitioner?_getpages=abc"}],
            }
        )

    monkeypatch.setattr(m, "_request_with_retry", fake_request_with_retry)

    pages = m.fetch_resources(
        client=object(),
        output_dir=tmp_path,
        resource_type="Practitioner",
        count=1000,
        hard_limit=None,
        retry=m.RetryConfig(),
        log_dir=tmp_path / "log",
        url_print=m.UrlPrintConfig(print_urls=False),
        curl_debug=m.CurlDebugConfig(curl_on_error=False),
        curl_auth=None,
    )

    page = next(pages)
    # We were already at the overrun threshold; should stop without yielding any resources.
    assert page.resources == []
    assert page.next_url is None

    captured = capsys.readouterr()
    assert "The server appears to have more data than it estimated" in (captured.out + captured.err)
