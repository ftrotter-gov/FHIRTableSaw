"""Persistence for Endpoint canonical model."""

from __future__ import annotations

import uuid

from sqlalchemy import select
from sqlalchemy.orm import Session

from fhir_tablesaw_3tier.db.models import EndpointPayloadTypeRow, EndpointRow
from fhir_tablesaw_3tier.db.persist_common import ensure_uuid
from fhir_tablesaw_3tier.domain.endpoint import Endpoint


def save_endpoint(session: Session, endpoint: Endpoint) -> Endpoint:
    euuid = ensure_uuid(endpoint.resource_uuid)

    row = session.execute(select(EndpointRow).where(EndpointRow.resource_uuid == euuid)).scalar_one_or_none()
    if row is None:
        row = EndpointRow(
            resource_uuid=euuid,
            status=endpoint.status,
            connection_type_code=endpoint.connection_type.code,
        )
        session.add(row)

    row.status = endpoint.status
    row.connection_type_system = endpoint.connection_type.system
    row.connection_type_code = endpoint.connection_type.code
    row.connection_type_display = endpoint.connection_type.display
    row.name = endpoint.name
    row.endpoint_rank = endpoint.endpoint_rank

    session.flush()
    endpoint_id = int(row.id)

    # payload types (insert-only dedupe)
    for p in endpoint.payload_types:
        existing = session.execute(
            select(EndpointPayloadTypeRow).where(
                EndpointPayloadTypeRow.endpoint_id == endpoint_id,
                EndpointPayloadTypeRow.payload_system == p.system,
                EndpointPayloadTypeRow.payload_code == p.code,
                EndpointPayloadTypeRow.payload_display == p.display,
            )
        ).scalar_one_or_none()
        if existing is None:
            session.add(
                EndpointPayloadTypeRow(
                    endpoint_id=endpoint_id,
                    payload_system=p.system,
                    payload_code=p.code,
                    payload_display=p.display,
                )
            )

    session.flush()
    return endpoint.model_copy(update={"id": endpoint_id})


def load_endpoint_by_uuid(session: Session, resource_uuid: str | uuid.UUID) -> Endpoint:
    euuid = ensure_uuid(resource_uuid)
    row = session.execute(select(EndpointRow).where(EndpointRow.resource_uuid == euuid)).scalar_one()

    payload_rows = session.execute(
        select(EndpointPayloadTypeRow).where(EndpointPayloadTypeRow.endpoint_id == int(row.id))
    ).scalars().all()

    payload_types = [
        {
            "system": p.payload_system,
            "code": p.payload_code,
            "display": p.payload_display,
        }
        for p in payload_rows
    ]

    return Endpoint(
        id=int(row.id),
        resource_uuid=str(row.resource_uuid),
        status=str(row.status),
        connection_type={
            "system": row.connection_type_system,
            "code": row.connection_type_code,
            "display": row.connection_type_display,
        },
        name=row.name,
        endpoint_rank=int(row.endpoint_rank) if row.endpoint_rank is not None else None,
        payload_types=payload_types,
    )
