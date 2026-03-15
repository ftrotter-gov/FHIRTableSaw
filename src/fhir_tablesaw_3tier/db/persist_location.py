"""Persistence for canonical Location."""

from __future__ import annotations

import uuid

from sqlalchemy import select
from sqlalchemy.orm import Session

from fhir_tablesaw_3tier.db.models import (
    AddressRow,
    EndpointRow,
    LocationAccessibilityRow,
    LocationHoursOfOperationRow,
    LocationRow,
    LocationTelecomRow,
    OrganizationRow,
    TelecomRow,
)
from fhir_tablesaw_3tier.db.persist_common import ensure_uuid, get_or_create_telecom_id
from fhir_tablesaw_3tier.domain.location import Location


def _ensure_org_registry_id(session: Session, org_uuid: str | uuid.UUID) -> int:
    ouuid = ensure_uuid(org_uuid)
    row = session.execute(
        select(OrganizationRow).where(OrganizationRow.resource_uuid == ouuid)
    ).scalar_one_or_none()
    if row is None:
        row = OrganizationRow(resource_uuid=ouuid, org_type_code=None)
        session.add(row)
        session.flush()
    return int(row.id)


def _ensure_location_id(session: Session, location_uuid: str | uuid.UUID) -> int:
    luuid = ensure_uuid(location_uuid)
    row = session.execute(select(LocationRow).where(LocationRow.resource_uuid == luuid)).scalar_one_or_none()
    if row is None:
        # Placeholder: required fields will be filled when we persist fully.
        row = LocationRow(resource_uuid=luuid, status="active", name="(placeholder)")
        session.add(row)
        session.flush()
    return int(row.id)


def _get_or_create_address_id(session: Session, loc: Location) -> int | None:
    if not any(
        [
            loc.address_line1,
            loc.address_line2,
            loc.address_city,
            loc.address_state,
            loc.address_postal_code,
            loc.address_country,
        ]
    ):
        return None

    row = AddressRow(
        line1=loc.address_line1,
        line2=loc.address_line2,
        city=loc.address_city,
        state=loc.address_state,
        postal_code=loc.address_postal_code,
        country=loc.address_country,
    )
    session.add(row)
    session.flush()
    return int(row.id)


def save_location(session: Session, loc: Location) -> Location:
    ruuid = ensure_uuid(loc.resource_uuid)

    row = session.execute(select(LocationRow).where(LocationRow.resource_uuid == ruuid)).scalar_one_or_none()
    if row is None:
        row = LocationRow(resource_uuid=ruuid, status=loc.status, name=loc.name)
        session.add(row)

    row.status = loc.status
    row.name = loc.name
    row.description = loc.description
    row.availability_exceptions = loc.availability_exceptions

    row.managing_organization_id = (
        _ensure_org_registry_id(session, loc.managing_organization_resource_uuid)
        if loc.managing_organization_resource_uuid
        else None
    )
    row.part_of_location_id = (
        _ensure_location_id(session, loc.part_of_location_resource_uuid)
        if loc.part_of_location_resource_uuid
        else None
    )

    row.address_id = _get_or_create_address_id(session, loc)

    row.latitude = str(loc.position.latitude) if loc.position is not None else None
    row.longitude = str(loc.position.longitude) if loc.position is not None else None
    row.altitude = (
        str(loc.position.altitude)
        if loc.position is not None and loc.position.altitude is not None
        else None
    )

    row.boundary_geojson = (
        loc.boundary_geojson.geojson_text if loc.boundary_geojson is not None else None
    )

    session.flush()
    location_id = int(row.id)

    # telecom joins
    for t in loc.telecoms:
        telecom_id = get_or_create_telecom_id(session, type=t.type, value=t.value)
        join = session.execute(
            select(LocationTelecomRow).where(
                LocationTelecomRow.location_id == location_id,
                LocationTelecomRow.telecom_id == telecom_id,
            )
        ).scalar_one_or_none()
        if join is None:
            session.add(LocationTelecomRow(location_id=location_id, telecom_id=telecom_id))

    # endpoints: ensure EndpointRow exists; we don't create join table yet
    for endpoint_uuid in loc.endpoints:
        euuid = ensure_uuid(endpoint_uuid)
        erow = session.execute(select(EndpointRow).where(EndpointRow.resource_uuid == euuid)).scalar_one_or_none()
        if erow is None:
            erow = EndpointRow(resource_uuid=euuid, raw_json=None)
            session.add(erow)
            session.flush()

    # hours of operation
    for h in loc.hours_of_operation:
        session.add(
            LocationHoursOfOperationRow(
                location_id=location_id,
                all_day=h.all_day,
                opening_time=h.opening_time,
                closing_time=h.closing_time,
            )
        )

    # accessibility
    for a in loc.accessibility:
        session.add(
            LocationAccessibilityRow(
                location_id=location_id,
                code_system=a.system,
                code_code=a.code,
                code_display=a.display,
            )
        )

    session.flush()
    return loc.model_copy(update={"id": location_id})


def load_location_by_uuid(session: Session, resource_uuid: str | uuid.UUID) -> Location:
    ruuid = ensure_uuid(resource_uuid)
    row = session.execute(select(LocationRow).where(LocationRow.resource_uuid == ruuid)).scalar_one()

    # address
    addr = None
    if row.address_id is not None:
        addr = session.execute(select(AddressRow).where(AddressRow.id == row.address_id)).scalar_one()

    # telecoms
    tel_rows = session.execute(
        select(LocationTelecomRow, TelecomRow)
        .where(LocationTelecomRow.location_id == int(row.id))
        .where(LocationTelecomRow.telecom_id == TelecomRow.id)
    ).all()
    telecoms = [{"type": str(t.type), "value": str(t.value)} for _, t in tel_rows]

    # hours
    hours_rows = session.execute(
        select(LocationHoursOfOperationRow).where(
            LocationHoursOfOperationRow.location_id == int(row.id)
        )
    ).scalars().all()
    hours = [
        {
            "all_day": h.all_day,
            "opening_time": h.opening_time,
            "closing_time": h.closing_time,
        }
        for h in hours_rows
    ]

    # accessibility
    acc_rows = session.execute(
        select(LocationAccessibilityRow).where(LocationAccessibilityRow.location_id == int(row.id))
    ).scalars().all()
    accessibility = [
        {"system": a.code_system, "code": a.code_code, "display": a.code_display}
        for a in acc_rows
    ]

    managing_org_uuid = None
    if row.managing_organization_id is not None:
        org = session.execute(
            select(OrganizationRow).where(OrganizationRow.id == row.managing_organization_id)
        ).scalar_one()
        managing_org_uuid = str(org.resource_uuid)

    part_of_uuid = None
    if row.part_of_location_id is not None:
        parent = session.execute(
            select(LocationRow).where(LocationRow.id == row.part_of_location_id)
        ).scalar_one()
        part_of_uuid = str(parent.resource_uuid)

    position = None
    if row.latitude is not None and row.longitude is not None:
        try:
            position = {
                "latitude": float(str(row.latitude)),
                "longitude": float(str(row.longitude)),
                "altitude": float(str(row.altitude)) if row.altitude is not None else None,
            }
        except ValueError:
            position = None

    boundary = None
    if row.boundary_geojson is not None:
        boundary = {"geojson_text": str(row.boundary_geojson)}

    return Location(
        id=int(row.id),
        resource_uuid=str(row.resource_uuid),
        status=str(row.status),
        name=str(row.name),
        description=row.description,
        availability_exceptions=row.availability_exceptions,
        managing_organization_resource_uuid=managing_org_uuid,
        part_of_location_resource_uuid=part_of_uuid,
        address_line1=str(addr.line1) if addr and addr.line1 is not None else None,
        address_line2=str(addr.line2) if addr and addr.line2 is not None else None,
        address_city=str(addr.city) if addr and addr.city is not None else None,
        address_state=str(addr.state) if addr and addr.state is not None else None,
        address_postal_code=str(addr.postal_code) if addr and addr.postal_code is not None else None,
        address_country=str(addr.country) if addr and addr.country is not None else None,
        position=position,
        boundary_geojson=boundary,
        accessibility=accessibility,
        telecoms=telecoms,
        # endpoints not yet joined
        endpoints=[],
        hours_of_operation=hours,
        newpatients=[],
        verification_status=None,
    )
