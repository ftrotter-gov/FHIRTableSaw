from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from fhir_tablesaw_3tier.db.base import Base
from fhir_tablesaw_3tier.db.persist_organization_affiliation import (
    load_organization_affiliation_by_uuid,
    save_organization_affiliation,
)
from fhir_tablesaw_3tier.db.persist_organization_clinical import (
    load_clinical_organization_by_uuid,
    save_clinical_organization,
)
from fhir_tablesaw_3tier.fhir.organization_affiliation import (
    organization_affiliation_from_fhir_json,
)
from fhir_tablesaw_3tier.fhir.organization_clinical import clinical_organization_from_fhir_json


def test_persist_and_load_clinical_org_and_org_affiliation_sqlite() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        # create two orgs
        o1_raw = {
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
            "telecom": [{"system": "phone", "value": "555-0000"}],
        }
        o2_raw = {
            "resourceType": "Organization",
            "id": "33333333-3333-3333-3333-333333333333",
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
                {"system": "http://hl7.org/fhir/sid/us-npi", "value": "9999999999"}
            ],
            "telecom": [{"system": "fax", "value": "555-9999"}],
        }

        o1, _ = clinical_organization_from_fhir_json(o1_raw)
        o2, _ = clinical_organization_from_fhir_json(o2_raw)

        o1 = save_clinical_organization(session, o1)
        o2 = save_clinical_organization(session, o2)
        session.commit()

        o1_loaded = load_clinical_organization_by_uuid(session, o1.resource_uuid)
        assert o1_loaded.resource_uuid == o1.resource_uuid
        assert o1_loaded.npi == "1234567890"

        # affiliation
        aff_raw = {
            "resourceType": "OrganizationAffiliation",
            "id": "44444444-4444-4444-4444-444444444444",
            "active": True,
            "organization": {"reference": f"Organization/{o1.resource_uuid}"},
            "participatingOrganization": {"reference": f"Organization/{o2.resource_uuid}"},
            "code": [
                {
                    "coding": [
                        {"system": "http://example.com/roles", "code": "role1"}
                    ]
                }
            ],
            "specialty": [{"coding": [{"code": "207Q00000X"}]}],
            "telecom": [
                {"system": "phone", "value": "555-1212"},
                {"system": "email", "value": "drop@example.com"},
            ],
        }

        aff, report = organization_affiliation_from_fhir_json(aff_raw)
        assert report.dropped_counts.get("OrganizationAffiliation.telecom") == 1

        aff = save_organization_affiliation(session, aff)
        session.commit()

        loaded = load_organization_affiliation_by_uuid(session, aff.resource_uuid)
        assert loaded.primary_organization_resource_uuid == o1.resource_uuid
        assert loaded.participating_organization_resource_uuid == o2.resource_uuid
        assert loaded.code.code == "role1"
        assert loaded.specialties[0].code == "207Q00000X"
        assert loaded.telecoms[0].type == "phone"
        assert loaded.telecoms[0].value == "555-1212"
