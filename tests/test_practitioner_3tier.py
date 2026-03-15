from __future__ import annotations


from fhir_tablesaw_3tier.fhir.practitioner import (
    practitioner_from_fhir_json,
    practitioner_to_fhir_json,
)


def test_parse_practitioner_minimal_and_dropped_repeats() -> None:
    raw = {
        "resourceType": "Practitioner",
        "id": "11111111-1111-1111-1111-111111111111",
        "identifier": [
            {"system": "http://hl7.org/fhir/sid/us-npi", "value": "1234567890"}
        ],
        "active": True,
        "name": [
            {"family": "Smith", "given": ["Jane", "A.", "Extra"], "prefix": ["Dr", "X"]},
            {"family": "Smith", "given": ["Janie"]},
            {"family": "DropMe", "given": ["TooMany"]},
        ],
        "telecom": [
            {"system": "phone", "value": "555-1111"},
            {"system": "email", "value": "jane@example.com"},
        ],
        "address": [
            {
                "line": ["L1", "L2", "L3"],
                "city": "Somewhere",
                "state": "NY",
                "postalCode": "12345",
            }
        ],
        "specialty": [{"coding": [{"code": "207Q00000X"}]}],
        "qualification": [{"code": {"text": "MD"}}],
    }

    practitioner, report = practitioner_from_fhir_json(raw)
    assert practitioner.resource_uuid == "11111111-1111-1111-1111-111111111111"
    assert practitioner.npi == "1234567890"
    assert practitioner.first_name == "Jane"
    assert practitioner.middle_name == "A."
    assert practitioner.last_name == "Smith"

    # specialty -> clinician types
    assert practitioner.clinician_types[0].code == "207Q00000X"
    # qualification -> credentials
    assert practitioner.credentials[0].value == "MD"

    # telecom email dropped
    assert any(t.type == "phone" for t in practitioner.telecoms)
    assert report.dropped_counts.get("Practitioner.telecom") == 1

    # names dropped
    assert report.dropped_counts.get("Practitioner.name") == 1
    assert report.dropped_counts.get("HumanName.given") == 1
    assert report.dropped_counts.get("HumanName.prefix") == 1

    # address lines dropped
    assert report.dropped_counts.get("Address.line") == 1


def test_practitioner_to_fhir_json_smoke() -> None:
    raw = {
        "resourceType": "Practitioner",
        "id": "11111111-1111-1111-1111-111111111111",
        "identifier": [
            {"system": "http://hl7.org/fhir/sid/us-npi", "value": "1234567890"}
        ],
        "active": True,
        "name": [{"family": "Smith", "given": ["Jane"]}],
        "specialty": [{"coding": [{"code": "207Q00000X"}]}],
        "qualification": [{"code": {"text": "MD"}}],
    }

    practitioner, _ = practitioner_from_fhir_json(raw)
    out = practitioner_to_fhir_json(practitioner)
    assert out["resourceType"] == "Practitioner"
    assert out["id"] == "11111111-1111-1111-1111-111111111111"
    assert out["identifier"][0]["system"] == "http://hl7.org/fhir/sid/us-npi"
    assert out["identifier"][0]["value"] == "1234567890"
    assert out["specialty"][0]["coding"][0]["code"] == "207Q00000X"

