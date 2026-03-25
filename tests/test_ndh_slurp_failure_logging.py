from __future__ import annotations

import json
import csv

from fhir_tablesaw_3tier.ndh_slurp import _safe_parse, log_failure


def test_log_failure_writes_artifacts(tmp_path) -> None:
    raw = {
        "resourceType": "Practitioner",
        "id": "bad/id with spaces",
        "identifier": [],
    }

    ex = ValueError("boom")
    log_failure(
        log_dir=tmp_path,
        fhir_object_type="Practitioner",
        fhir_resource_id=str(raw["id"]),
        failed_json=raw,
        exc=ex,
        stage="parse",
    )

    # path should be sanitized
    out_dir = tmp_path / "Practitioner" / "bad_id_with_spaces"
    assert (out_dir / "failed.json").exists()
    assert (out_dir / "whatwentwrong.log").exists()

    loaded = json.loads((out_dir / "failed.json").read_text(encoding="utf-8"))
    assert loaded["resourceType"] == "Practitioner"
    assert loaded["id"] == "bad/id with spaces"

    log_txt = (out_dir / "whatwentwrong.log").read_text(encoding="utf-8")
    assert "stage: parse" in log_txt
    assert "exception: ValueError: boom" in log_txt


def test_safe_parse_consolidates_missing_npi_for_practitioner(tmp_path) -> None:
    raw = {
        "resourceType": "Practitioner",
        "id": "PractitionerOneWithNetwork1AndNetwork2",
        "identifier": [],
    }

    def parse_fn(_raw, *, fhir_server_url=None):
        raise ValueError("Practitioner is missing required NPI identifier")

    dropped = {}
    failures = {}
    out = _safe_parse(
        parse_fn,
        raw,
        dropped_counter=dropped,  # type: ignore[arg-type]
        failures_counter=failures,  # type: ignore[arg-type]
        label="Practitioner",
        fhir_server_url=None,
        log_dir=tmp_path,
    )
    assert out is None

    csv_path = tmp_path / "Practitioner" / "missing_an_npi.csv"
    assert csv_path.exists()

    rows = list(csv.reader(csv_path.read_text(encoding="utf-8").splitlines()))
    assert rows[0] == ["failing_uuids"]
    assert rows[1] == [raw["id"]]

    # Should NOT have created a per-resource directory for this common failure.
    assert not (tmp_path / "Practitioner" / raw["id"]).exists()


def test_safe_parse_consolidates_missing_practitionerrole_refs(tmp_path) -> None:
    raw = {
        "resourceType": "PractitionerRole",
        "id": "f21a1b4b-0fd1-4ec6-8b89-ef3f5a7d9c6f",
    }

    def parse_fn(_raw, *, fhir_server_url=None):
        raise ValueError("PractitionerRole requires practitioner and organization references")

    out = _safe_parse(
        parse_fn,
        raw,
        dropped_counter={},  # type: ignore[arg-type]
        failures_counter={},  # type: ignore[arg-type]
        label="PractitionerRole",
        fhir_server_url=None,
        log_dir=tmp_path,
    )
    assert out is None

    csv_path = tmp_path / "PractitionerRole" / "missing_practicioner_and_organization.csv"
    assert csv_path.exists()

    rows = list(csv.reader(csv_path.read_text(encoding="utf-8").splitlines()))
    assert rows[0] == ["failing_uuids"]
    assert rows[1] == [raw["id"]]
