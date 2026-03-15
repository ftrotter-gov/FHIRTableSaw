from __future__ import annotations


from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from fhir_tablesaw_3tier.db.base import Base
from fhir_tablesaw_3tier.db.persist_organization_clinical import save_clinical_organization
from fhir_tablesaw_3tier.db.persist_practitioner import save_practitioner
from fhir_tablesaw_3tier.db.persist_practitioner_role import (
    load_practitioner_role_by_uuid,
    save_practitioner_role,
)
from fhir_tablesaw_3tier.fhir.organization_clinical import clinical_organization_from_fhir_json
from fhir_tablesaw_3tier.fhir.practitioner import practitioner_from_fhir_json
from fhir_tablesaw_3tier.fhir.practitioner_role import (
    practitioner_role_from_fhir_json,
    practitioner_role_to_fhir_json,
)


def test_parse_practitioner_role_and_drops() -> None:
    raw = {
        "resourceType": "PractitionerRole",
        "id": "55555555-5555-5555-5555-555555555555",
        "active": True,
        "practitioner": {"reference": "Practitioner/11111111-1111-1111-1111-111111111111"},
        "organization": {"reference": "Organization/22222222-2222-2222-2222-222222222222"},
        "code": [
            {
                "coding": [
                    {
                        "system": "http://example.com/role-system",
                        "code": "doc",
                        "display": "Doctor",
                    }
                ]
            },
            {"coding": [{"code": "drop"}]},
        ],
        "specialty": [
            {"coding": [{"code": "207Q00000X"}]},
            {"text": "207R00000X"},
        ],
        "telecom": [
            {"system": "phone", "value": "555-1111"},
            {"system": "email", "value": "drop@example.com"},
        ],
        "endpoint": [{"reference": "Endpoint/eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"}],
        "location": [
            {"reference": "Location/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"},
            {"reference": "Location/drop"},
        ],
        "healthcareService": [
            {"reference": "HealthcareService/bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"},
            {"reference": "HealthcareService/drop"},
        ],
        "extension": [
            {
                "url": "https://example.com/extension_url/accepting_new_patients",
                "valueBoolean": True,
            },
            {
                "url": "https://example.com/extension_url/practitioner_role_rating",
                "valueInteger": 4,
            },
            {
                "url": "https://example.com/extension_url/practitioner_role_cms_pecos_validated",
                "valueBoolean": True,
            },
        ],
    }

    role, report = practitioner_role_from_fhir_json(raw)
    assert role.resource_uuid == "55555555-5555-5555-5555-555555555555"
    assert role.code.code == "doc"
    assert role.specialties[0].code == "207Q00000X"
    assert role.accepting_new_patients is True
    assert role.rating == 4
    assert role.cms_pecos_validated is True
    assert report.dropped_counts.get("PractitionerRole.code") == 1
    assert report.dropped_counts.get("PractitionerRole.telecom") == 1
    assert report.dropped_counts.get("PractitionerRole.location") == 1
    assert report.dropped_counts.get("PractitionerRole.healthcareService") == 1

    out = practitioner_role_to_fhir_json(role)
    assert out["resourceType"] == "PractitionerRole"
    assert out["id"] == "55555555-5555-5555-5555-555555555555"
    assert out["active"] is True


def test_persist_and_load_practitioner_role_sqlite() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        pract_raw = {
            "resourceType": "Practitioner",
            "id": "11111111-1111-1111-1111-111111111111",
            "identifier": [
                {"system": "http://hl7.org/fhir/sid/us-npi", "value": "1234567890"}
            ],
        }
        org_raw = {
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

        pract, _ = practitioner_from_fhir_json(pract_raw)
        org, _ = clinical_organization_from_fhir_json(org_raw)
        pract = save_practitioner(session, pract)
        org = save_clinical_organization(session, org)
        session.commit()

        role_raw = {
            "resourceType": "PractitionerRole",
            "id": "55555555-5555-5555-5555-555555555555",
            "active": True,
            "practitioner": {"reference": f"Practitioner/{pract.resource_uuid}"},
            "organization": {"reference": f"Organization/{org.resource_uuid}"},
            "code": [{"coding": [{"code": "doc"}]}],
            "specialty": [{"coding": [{"code": "207Q00000X"}]}],
            "telecom": [{"system": "phone", "value": "555-1111"}],
            "endpoint": [{"reference": "Endpoint/eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"}],
            "location": [{"reference": "Location/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"}],
            "healthcareService": [
                {"reference": "HealthcareService/bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"}
            ],
        }

        role, _ = practitioner_role_from_fhir_json(role_raw)
        role = save_practitioner_role(session, role)
        session.commit()

        loaded = load_practitioner_role_by_uuid(session, role.resource_uuid)
        assert loaded.practitioner_resource_uuid == pract.resource_uuid
        assert loaded.organization_resource_uuid == org.resource_uuid
        assert loaded.code.code == "doc"
        assert loaded.specialties[0].code == "207Q00000X"
        assert loaded.telecoms[0].type == "phone"
        assert loaded.endpoints[0].resource_uuid == "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"
        assert loaded.location_resource_uuid == "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        assert loaded.healthcare_service_resource_uuid == "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
