from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from fhir_tablesaw_3tier.db.base import Base
from fhir_tablesaw_3tier.db.persist_endpoint import load_endpoint_by_uuid, save_endpoint
from fhir_tablesaw_3tier.fhir.endpoint import endpoint_from_fhir_json, endpoint_to_fhir_json


def test_parse_endpoint_and_roundtrip() -> None:
    raw = {
        "resourceType": "Endpoint",
        "id": "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee",
        "status": "active",
        "connectionType": {"system": "http://example.org", "code": "hl7-fhir-rest"},
        "name": "Test Endpoint",
        "extension": [
            {
                "url": "http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-endpoint-rank",
                "valueInteger": 3,
            }
        ],
        "payloadType": [
            {"coding": [{"system": "http://example.org/p", "code": "any"}]},
            {"text": "text-only"},
        ],
    }

    endpoint, report = endpoint_from_fhir_json(raw)
    assert endpoint.resource_uuid == "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"
    assert endpoint.status == "active"
    assert endpoint.connection_type.code == "hl7-fhir-rest"
    assert endpoint.endpoint_rank == 3
    assert len(endpoint.payload_types) == 2
    assert report.dropped_counts == {}

    rt = endpoint_to_fhir_json(endpoint)
    assert rt["resourceType"] == "Endpoint"
    assert rt["id"] == endpoint.resource_uuid
    assert rt["status"] == "active"
    assert rt["connectionType"]["code"] == "hl7-fhir-rest"


def test_persist_and_load_endpoint_sqlite() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        raw = {
            "resourceType": "Endpoint",
            "id": "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee",
            "status": "active",
            "connectionType": {"code": "hl7-fhir-rest"},
            "payloadType": [{"coding": [{"code": "any"}]}],
        }
        endpoint, _ = endpoint_from_fhir_json(raw)
        endpoint = save_endpoint(session, endpoint)
        session.commit()

        loaded = load_endpoint_by_uuid(session, endpoint.resource_uuid)
        assert loaded.resource_uuid == endpoint.resource_uuid
        assert loaded.status == "active"
        assert loaded.connection_type.code == "hl7-fhir-rest"
        assert len(loaded.payload_types) == 1
