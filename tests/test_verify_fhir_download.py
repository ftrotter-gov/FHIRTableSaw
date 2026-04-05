from __future__ import annotations

import json
from pathlib import Path

import httpx


def test_resource_type_from_filename():
    from verify_fhir_download import _resource_type_from_filename

    assert _resource_type_from_filename(Path("location.ndjson")) == "Location"
    assert _resource_type_from_filename(Path("organization_affiliation.ndjson")) == "OrganizationAffiliation"
    assert _resource_type_from_filename(Path("practitioner_role.ndjson")) == "PractitionerRole"


def test_compute_ndjson_stats_counts_unique_and_duplicates(tmp_path: Path):
    from verify_fhir_download import compute_ndjson_stats

    p = tmp_path / "location.ndjson"
    rows = [
        {"resourceType": "Location", "id": "a"},
        {"resourceType": "Location", "id": "b"},
        {"resourceType": "Location", "id": "a"},  # duplicate
        "not-json",  # parse error (written raw)
        {"resourceType": "Location"},  # missing id
        {"resourceType": "Organization", "id": "zzz"},  # wrong resourceType
    ]

    with p.open("w", encoding="utf-8") as f:
        for r in rows:
            if isinstance(r, str):
                f.write(r + "\n")
            else:
                f.write(json.dumps(r) + "\n")

    stats = compute_ndjson_stats(ndjson_path=p, resource_type="Location")
    assert stats.line_count == 6
    assert stats.unique_id_count == 2
    assert stats.duplicate_id_count == 1
    assert stats.parse_error_count == 2
    assert stats.missing_id_count == 1


def test_extract_total_from_bundle_variants():
    from verify_fhir_download import _extract_total_from_bundle

    assert _extract_total_from_bundle({"resourceType": "Bundle", "total": 12}) == 12
    assert _extract_total_from_bundle({"count": 34}) == 34
    assert _extract_total_from_bundle({"results": {"resourceType": "Bundle", "total": 56}}) == 56
    assert _extract_total_from_bundle({"resourceType": "Bundle"}) is None
    assert _extract_total_from_bundle([1, 2, 3]) is None


def test_fetch_expected_total_prefers_summary_count():
    from verify_fhir_download import fetch_expected_total

    def handler(request: httpx.Request) -> httpx.Response:
        # Ensure first attempt is _summary=count
        if request.url.params.get("_summary") == "count":
            return httpx.Response(200, json={"resourceType": "Bundle", "total": 99})
        return httpx.Response(500, json={"error": "should not be called"})

    transport = httpx.MockTransport(handler)
    with httpx.Client(base_url="https://example.org/fhir/", transport=transport) as client:
        out = fetch_expected_total(client=client, resource_type="Location")

    assert out.expected_total == 99
    assert out.method == "_summary=count"


def test_fetch_expected_total_falls_back_to_total_accurate():
    from verify_fhir_download import fetch_expected_total

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.params.get("_summary") == "count":
            return httpx.Response(400, json={"resourceType": "OperationOutcome"})
        if request.url.params.get("_count") == "0" and request.url.params.get("_total") == "accurate":
            return httpx.Response(200, json={"resourceType": "Bundle", "total": 123})
        return httpx.Response(500, json={"error": "unexpected"})

    transport = httpx.MockTransport(handler)
    with httpx.Client(base_url="https://example.org/fhir/", transport=transport) as client:
        out = fetch_expected_total(client=client, resource_type="Organization")

    assert out.expected_total == 123
    assert out.method == "_count=0&_total=accurate"


def test_write_csv_report_writes_rows(tmp_path: Path):
    from verify_fhir_download import write_csv_report

    nd = tmp_path / "nd"
    nd.mkdir()
    p = nd / "location.ndjson"
    p.write_text(
        "\n".join(
            [
                json.dumps({"resourceType": "Location", "id": "a"}),
                json.dumps({"resourceType": "Location", "id": "b"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"resourceType": "Bundle", "total": 2})

    transport = httpx.MockTransport(handler)
    with httpx.Client(base_url="https://example.org/fhir/", transport=transport) as client:
        csv_out = tmp_path / "report.csv"
        any_fail = write_csv_report(
            csv_out=csv_out,
            pairs=[("Location", p)],
            client=client,
            allow_delta=0,
            api_max_attempts_per_url=1,
            api_initial_timeout_seconds=0.1,
        )

    assert any_fail is False
    assert csv_out.exists()
    txt = csv_out.read_text(encoding="utf-8")
    assert "fhir_resource_type,resource_id_count_from_file,resource_id_count_from_url" in txt
    # row should include fhir_resource_type and both counts
    assert "Location" in txt
    assert ",2,2" in txt


def test_progress_prints_to_stderr(tmp_path: Path, capsys):
    """Progress messages should go to stderr so stdout can stay parseable."""

    from verify_fhir_download import main

    nd = tmp_path / "nd"
    nd.mkdir()
    (nd / "organization.ndjson").write_text(
        json.dumps({"resourceType": "Organization", "id": "o1"}) + "\n",
        encoding="utf-8",
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"resourceType": "Bundle", "total": 1})

    transport = httpx.MockTransport(handler)

    import verify_fhir_download as v

    orig_client = v.httpx.Client

    class _PatchedClient(orig_client):
        def __init__(self, *args, **kwargs):
            kwargs["transport"] = transport
            super().__init__(*args, **kwargs)

    # Patch httpx.Client inside the module for this test.
    v.httpx.Client = _PatchedClient  # type: ignore[assignment]
    try:
        # Run with explicit args and a temp csv path.
        csv_out = tmp_path / "out.csv"
        rc = main([str(nd), "https://example.org/fhir/", "--csv-out", str(csv_out)])
        assert rc == 0

        captured = capsys.readouterr()
        assert "Working on Organization" in captured.err
        assert "VERIFY_STATUS resource_type=Organization" in captured.err
        assert "Done Organization" in captured.err
        assert "timings: file_parse_seconds=" in captured.err
        assert "api_count_seconds=" in captured.err
        assert "Wrote CSV report" in captured.out
    finally:
        v.httpx.Client = orig_client  # type: ignore[assignment]
