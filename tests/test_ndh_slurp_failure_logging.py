from __future__ import annotations

import json

from fhir_tablesaw_3tier.ndh_slurp import log_failure


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
