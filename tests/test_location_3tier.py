from __future__ import annotations


from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from fhir_tablesaw_3tier.db.base import Base
from fhir_tablesaw_3tier.db.persist_location import load_location_by_uuid, save_location
from fhir_tablesaw_3tier.fhir.location import location_from_fhir_json, location_to_fhir_json


def test_parse_location_and_roundtrip_boundary_accessibility() -> None:
    # boundary: attachment.data must be base64
    import base64

    geo = '{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,0]]]}'
    b64 = base64.b64encode(geo.encode("utf-8")).decode("ascii")

    raw = {
        "resourceType": "Location",
        "id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        "status": "active",
        "name": "Test Clinic",
        "address": {"line": ["1 Main"], "city": "Springfield", "state": "MA"},
        "position": {"latitude": 42.0, "longitude": -71.0},
        "telecom": [
            {"system": "phone", "value": "555-1111"},
            {"system": "email", "value": "drop@example.com"},
        ],
        "extension": [
            {
                "url": "http://hl7.org/fhir/StructureDefinition/location-boundary-geojson",
                "valueAttachment": {"contentType": "application/geo+json", "data": b64},
            },
            {
                "url": "http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-accessibility",
                "valueCodeableConcept": {"coding": [{"code": "wheel"}]},
            },
        ],
    }

    loc, report = location_from_fhir_json(raw)
    assert loc.resource_uuid == "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    assert loc.boundary_geojson is not None
    assert geo in loc.boundary_geojson.geojson_text
    assert len(loc.accessibility) == 1
    assert report.dropped_counts.get("Location.telecom") == 1

    out = location_to_fhir_json(loc)
    assert out["resourceType"] == "Location"
    assert out["id"] == "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    assert out["status"] == "active"


def test_persist_and_load_location_sqlite() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    raw = {
        "resourceType": "Location",
        "id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        "status": "active",
        "name": "Test Clinic",
        "address": {"line": ["1 Main"], "city": "Springfield", "state": "MA"},
        "extension": [
            {
                "url": "http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-accessibility",
                "valueCodeableConcept": {"coding": [{"code": "wheel"}]},
            }
        ],
    }
    loc, _ = location_from_fhir_json(raw)

    with Session(engine) as session:
        loc = save_location(session, loc)
        session.commit()

        loaded = load_location_by_uuid(session, loc.resource_uuid)
        assert loaded.name == "Test Clinic"
        assert loaded.address_city == "Springfield"
        assert loaded.accessibility[0].code == "wheel"
