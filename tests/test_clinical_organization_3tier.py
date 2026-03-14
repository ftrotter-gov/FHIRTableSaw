from __future__ import annotations


from fhir_tablesaw_3tier.fhir.organization_clinical import (
    clinical_organization_from_fhir_json,
    clinical_organization_to_fhir_json,
)


def test_parse_clinical_organization_and_drops() -> None:
    raw = {
        "resourceType": "Organization",
        "id": "22222222-2222-2222-2222-222222222222",
        "meta": {"profile": ["http://hl7.org/fhir/us/ndh/StructureDefinition/ndh-Organization"]},
        "type": [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/organization-type",
                        "code": "prov",
                    }
                ]
            }
        ],
        "name": "Example Health Clinic LLC",
        "alias": ["Example Clinic"],
        "_alias": [
            {
                "extension": [
                    {
                        "url": "https://example.com/extension_url/org-alias-type",
                        "valueCode": "doing-business-as",
                    }
                ]
            }
        ],
        "identifier": [
            {"system": "http://hl7.org/fhir/sid/us-npi", "value": "1234567890"}
        ],
        "telecom": [
            {"system": "phone", "value": "555-0000"},
            {"system": "email", "value": "ignored@example.com"},
        ],
        "address": [
            {"line": ["L1", "L2", "L3"], "city": "X", "state": "NY", "postalCode": "1"}
        ],
        "contact": [
            {
                "name": {"family": "Johnson", "given": ["Emily"]},
                "telecom": [
                    {"system": "phone", "value": "7035551212"},
                    {"system": "email", "value": "nope@example.com"},
                ],
                "address": {"line": ["C1", "C2", "C3"], "city": "Y"},
            },
            {"name": {"family": "Drop"}},
        ],
        "endpoint": [{"reference": "Endpoint/eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"}],
        "extension": [
            {
                "url": "https://build.fhir.org/ig/HL7/fhir-us-ndh/StructureDefinition-base-ext-logo.html",
                "valueUrl": "https://example.com/logo.png",
            },
            {"url": "https://example.com/extension_url/rating", "valueInteger": 5},
            {
                "url": "https://example.com/extension_url/cms_pecos_validated",
                "valueBoolean": True,
            },
        ],
    }

    org, report = clinical_organization_from_fhir_json(raw)
    assert org.id == "22222222-2222-2222-2222-222222222222"
    assert org.npi == "1234567890"
    assert org.aliases[0].alias_type == "doing-business-as"
    assert org.logo_url == "https://example.com/logo.png"
    assert org.rating == 5
    assert org.cms_pecos_validated is True

    # drops
    assert report.dropped_counts.get("Organization.telecom") == 1
    assert report.dropped_counts.get("Organization.address.line") == 1
    assert report.dropped_counts.get("Organization.contact") == 1
    assert report.dropped_counts.get("Organization.contact.telecom") == 1
    assert report.dropped_counts.get("Organization.contact.address.line") == 1


def test_clinical_organization_to_fhir_json_smoke() -> None:
    raw = {
        "resourceType": "Organization",
        "id": "22222222-2222-2222-2222-222222222222",
        "type": [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/organization-type",
                        "code": "prov",
                    }
                ]
            }
        ],
        "identifier": [
            {"system": "http://hl7.org/fhir/sid/us-npi", "value": "1234567890"}
        ],
    }
    org, _ = clinical_organization_from_fhir_json(raw)
    out = clinical_organization_to_fhir_json(org)
    assert out["resourceType"] == "Organization"
    assert out["id"] == "22222222-2222-2222-2222-222222222222"
    assert out["type"][0]["coding"][0]["code"] == "prov"
