"""Persistence for OrganizationAffiliation canonical model."""

from __future__ import annotations

import uuid

from sqlalchemy import select
from sqlalchemy.orm import Session

from fhir_tablesaw_3tier.db.models import (
    EndpointRow,
    OrganizationAffiliationEndpointRow,
    OrganizationAffiliationRow,
    OrganizationAffiliationSpecialtyRow,
    OrganizationAffiliationTelecomRow,
    OrganizationRow,
    SpecialtyRow,
)
from fhir_tablesaw_3tier.db.persist_common import ensure_uuid, get_or_create_telecom_id
from fhir_tablesaw_3tier.domain.organization_affiliation import OrganizationAffiliation


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


def save_organization_affiliation(
    session: Session, aff: OrganizationAffiliation
) -> OrganizationAffiliation:
    auuid = ensure_uuid(aff.resource_uuid)

    primary_id = _ensure_org_registry_id(session, aff.primary_organization_resource_uuid)
    participating_id = _ensure_org_registry_id(
        session, aff.participating_organization_resource_uuid
    )

    row = session.execute(
        select(OrganizationAffiliationRow).where(OrganizationAffiliationRow.resource_uuid == auuid)
    ).scalar_one_or_none()
    if row is None:
        row = OrganizationAffiliationRow(
            resource_uuid=auuid,
            primary_organization_id=primary_id,
            participating_organization_id=participating_id,
            code_code=aff.code.code,
        )
        session.add(row)

    row.active = aff.active
    row.primary_organization_id = primary_id
    row.participating_organization_id = participating_id
    row.code_system = aff.code.system
    row.code_code = aff.code.code
    row.code_display = aff.code.display

    session.flush()
    aff_id = int(row.id)

    # specialties: normalized
    for s in aff.specialties:
        srow = session.execute(select(SpecialtyRow).where(SpecialtyRow.code == s.code)).scalar_one_or_none()
        if srow is None:
            srow = SpecialtyRow(code=s.code)
            session.add(srow)
            session.flush()
        join = session.execute(
            select(OrganizationAffiliationSpecialtyRow).where(
                OrganizationAffiliationSpecialtyRow.organization_affiliation_id == aff_id,
                OrganizationAffiliationSpecialtyRow.specialty_id == int(srow.id),
            )
        ).scalar_one_or_none()
        if join is None:
            session.add(
                OrganizationAffiliationSpecialtyRow(
                    organization_affiliation_id=aff_id,
                    specialty_id=int(srow.id),
                )
            )

    # telecoms: normalized
    for t in aff.telecoms:
        telecom_id = get_or_create_telecom_id(session, type=t.type, value=t.value)
        join = session.execute(
            select(OrganizationAffiliationTelecomRow).where(
                OrganizationAffiliationTelecomRow.organization_affiliation_id == aff_id,
                OrganizationAffiliationTelecomRow.telecom_id == telecom_id,
            )
        ).scalar_one_or_none()
        if join is None:
            session.add(
                OrganizationAffiliationTelecomRow(
                    organization_affiliation_id=aff_id,
                    telecom_id=telecom_id,
                )
            )

    # endpoints (if present): ensure endpoint rows exist + join
    for e in getattr(aff, "endpoints", []) or []:
        endpoint_uuid = e.get("resource_uuid") if isinstance(e, dict) else getattr(e, "resource_uuid", None)
        if not endpoint_uuid:
            continue
        euuid = ensure_uuid(endpoint_uuid)

        erow = session.execute(
            select(EndpointRow).where(EndpointRow.resource_uuid == euuid)
        ).scalar_one_or_none()
        if erow is None:
            erow = EndpointRow(
                resource_uuid=euuid,
                status="active",
                connection_type_code="(placeholder)",
            )
            session.add(erow)
            session.flush()

        ejoin = session.execute(
            select(OrganizationAffiliationEndpointRow).where(
                OrganizationAffiliationEndpointRow.organization_affiliation_id == aff_id,
                OrganizationAffiliationEndpointRow.endpoint_id == int(erow.id),
            )
        ).scalar_one_or_none()
        if ejoin is None:
            session.add(
                OrganizationAffiliationEndpointRow(
                    organization_affiliation_id=aff_id,
                    endpoint_id=int(erow.id),
                )
            )

    session.flush()
    return aff.model_copy(
        update={
            "id": aff_id,
            "primary_organization_id": primary_id,
            "participating_organization_id": participating_id,
        }
    )


def load_organization_affiliation_by_uuid(
    session: Session, resource_uuid: str | uuid.UUID
) -> OrganizationAffiliation:
    auuid = ensure_uuid(resource_uuid)
    row = session.execute(
        select(OrganizationAffiliationRow).where(OrganizationAffiliationRow.resource_uuid == auuid)
    ).scalar_one()

    # minimal load
    primary_org = session.execute(
        select(OrganizationRow).where(OrganizationRow.id == row.primary_organization_id)
    ).scalar_one()
    participating_org = session.execute(
        select(OrganizationRow).where(OrganizationRow.id == row.participating_organization_id)
    ).scalar_one()

    # specialties
    spec_rows = session.execute(
        select(OrganizationAffiliationSpecialtyRow, SpecialtyRow)
        .where(OrganizationAffiliationSpecialtyRow.organization_affiliation_id == int(row.id))
        .where(OrganizationAffiliationSpecialtyRow.specialty_id == SpecialtyRow.id)
    ).all()

    specialties = [{"code": str(s.code)} for _, s in spec_rows]

    # telecoms
    from fhir_tablesaw_3tier.db.models import TelecomRow

    tel_rows = session.execute(
        select(OrganizationAffiliationTelecomRow, TelecomRow)
        .where(OrganizationAffiliationTelecomRow.organization_affiliation_id == int(row.id))
        .where(OrganizationAffiliationTelecomRow.telecom_id == TelecomRow.id)
    ).all()

    telecoms = [{"type": str(t.type), "value": str(t.value)} for _, t in tel_rows]

    # endpoints
    ep_rows = session.execute(
        select(OrganizationAffiliationEndpointRow, EndpointRow)
        .where(OrganizationAffiliationEndpointRow.organization_affiliation_id == int(row.id))
        .where(OrganizationAffiliationEndpointRow.endpoint_id == EndpointRow.id)
    ).all()

    endpoints = [{"resource_uuid": str(e.resource_uuid)} for _, e in ep_rows]

    return OrganizationAffiliation(
        id=int(row.id),
        resource_uuid=str(row.resource_uuid),
        active=row.active,
        primary_organization_id=int(row.primary_organization_id),
        primary_organization_resource_uuid=str(primary_org.resource_uuid),
        participating_organization_id=int(row.participating_organization_id),
        participating_organization_resource_uuid=str(participating_org.resource_uuid),
        code={
            "system": row.code_system,
            "code": row.code_code,
            "display": row.code_display,
        },
        specialties=specialties,
        telecoms=telecoms,
        endpoints=endpoints,
    )
