"""Persistence for PractitionerRole canonical model."""

from __future__ import annotations

import uuid

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from fhir_tablesaw_3tier.db.models import (
    EndpointRow,
    EndpointPayloadTypeRow,
    HealthcareServiceRow,
    LocationRow,
    OrganizationRow,
    PractitionerRoleEndpointRow,
    PractitionerRoleRow,
    PractitionerRoleSpecialtyRow,
    PractitionerRoleTelecomRow,
    PractitionerRow,
    SpecialtyRow,
)
from fhir_tablesaw_3tier.db.persist_common import ensure_uuid, get_or_create_telecom_id
from fhir_tablesaw_3tier.db.upsert import execute_returning_scalar, is_postgres
from fhir_tablesaw_3tier.domain.practitioner_role import PractitionerRole


def _ensure_practitioner_id(session: Session, pract_uuid: str | uuid.UUID) -> int:
    puuid = ensure_uuid(pract_uuid)
    prow = session.execute(
        select(PractitionerRow).where(PractitionerRow.resource_uuid == puuid)
    ).scalar_one_or_none()
    if prow is None:
        raise ValueError(
            f"Practitioner {pract_uuid} must be persisted before PractitionerRole"
        )
    return int(prow.id)


def _ensure_org_registry_id(session: Session, org_uuid: str | uuid.UUID) -> int:
    ouuid = ensure_uuid(org_uuid)
    if is_postgres(session):
        stmt = (
            pg_insert(OrganizationRow)
            .values(resource_uuid=ouuid, org_type_code=None)
            .on_conflict_do_update(
                index_elements=[OrganizationRow.resource_uuid],
                set_={"resource_uuid": ouuid},
            )
            .returning(OrganizationRow.id)
        )
        return int(execute_returning_scalar(session, stmt))

    orow = session.execute(
        select(OrganizationRow).where(OrganizationRow.resource_uuid == ouuid)
    ).scalar_one_or_none()
    if orow is None:
        orow = OrganizationRow(resource_uuid=ouuid, org_type_code=None)
        session.add(orow)
        session.flush()
    return int(orow.id)


def _ensure_location_id(session: Session, location_uuid: str | uuid.UUID) -> int:
    luuid = ensure_uuid(location_uuid)
    if is_postgres(session):
        stmt = (
            pg_insert(LocationRow)
            .values(resource_uuid=luuid, status="active", name="(placeholder)")
            .on_conflict_do_update(
                index_elements=[LocationRow.resource_uuid],
                set_={"resource_uuid": luuid},
            )
            .returning(LocationRow.id)
        )
        return int(execute_returning_scalar(session, stmt))

    row = session.execute(
        select(LocationRow).where(LocationRow.resource_uuid == luuid)
    ).scalar_one_or_none()
    if row is None:
        # Location now has required status/name columns. Use placeholders when
        # referenced before full Location persistence.
        row = LocationRow(resource_uuid=luuid, status="active", name="(placeholder)")
        session.add(row)
        session.flush()
    return int(row.id)


def _ensure_healthcare_service_id(session: Session, hs_uuid: str | uuid.UUID) -> int:
    huuid = ensure_uuid(hs_uuid)
    if is_postgres(session):
        stmt = (
            pg_insert(HealthcareServiceRow)
            .values(resource_uuid=huuid, raw_json=None)
            .on_conflict_do_update(
                index_elements=[HealthcareServiceRow.resource_uuid],
                set_={"resource_uuid": huuid},
            )
            .returning(HealthcareServiceRow.id)
        )
        return int(execute_returning_scalar(session, stmt))

    row = session.execute(
        select(HealthcareServiceRow).where(HealthcareServiceRow.resource_uuid == huuid)
    ).scalar_one_or_none()
    if row is None:
        row = HealthcareServiceRow(resource_uuid=huuid, raw_json=None)
        session.add(row)
        session.flush()
    return int(row.id)


def save_practitioner_role(session: Session, role: PractitionerRole) -> PractitionerRole:
    ruuid = ensure_uuid(role.resource_uuid)

    practitioner_id = _ensure_practitioner_id(session, role.practitioner_resource_uuid)
    organization_id = _ensure_org_registry_id(session, role.organization_resource_uuid)

    location_id = (
        _ensure_location_id(session, role.location_resource_uuid)
        if role.location_resource_uuid
        else None
    )
    healthcare_service_id = (
        _ensure_healthcare_service_id(session, role.healthcare_service_resource_uuid)
        if role.healthcare_service_resource_uuid
        else None
    )

    if is_postgres(session):
        stmt = (
            pg_insert(PractitionerRoleRow)
            .values(
                resource_uuid=ruuid,
                active=role.active,
                practitioner_id=practitioner_id,
                organization_id=organization_id,
                code_system=role.code.system,
                code_code=role.code.code,
                code_display=role.code.display,
                accepting_new_patients=role.accepting_new_patients,
                rating=role.rating,
                cms_pecos_validated=role.cms_pecos_validated,
                cms_ial2_validated=role.cms_ial2_validated,
                has_cms_aligned_data_network=role.has_cms_aligned_data_network,
                location_id=location_id,
                healthcare_service_id=healthcare_service_id,
            )
            .on_conflict_do_update(
                index_elements=[PractitionerRoleRow.resource_uuid],
                set_={
                    "active": role.active,
                    "practitioner_id": practitioner_id,
                    "organization_id": organization_id,
                    "code_system": role.code.system,
                    "code_code": role.code.code,
                    "code_display": role.code.display,
                    "accepting_new_patients": role.accepting_new_patients,
                    "rating": role.rating,
                    "cms_pecos_validated": role.cms_pecos_validated,
                    "cms_ial2_validated": role.cms_ial2_validated,
                    "has_cms_aligned_data_network": role.has_cms_aligned_data_network,
                    "location_id": location_id,
                    "healthcare_service_id": healthcare_service_id,
                },
            )
            .returning(PractitionerRoleRow.id)
        )
        role_id = int(execute_returning_scalar(session, stmt))
    else:
        row = session.execute(
            select(PractitionerRoleRow).where(PractitionerRoleRow.resource_uuid == ruuid)
        ).scalar_one_or_none()
        if row is None:
            row = PractitionerRoleRow(
                resource_uuid=ruuid,
                practitioner_id=practitioner_id,
                organization_id=organization_id,
                code_code=role.code.code,
            )
            session.add(row)

        row.active = role.active
        row.practitioner_id = practitioner_id
        row.organization_id = organization_id

        row.code_system = role.code.system
        row.code_code = role.code.code
        row.code_display = role.code.display

        row.accepting_new_patients = role.accepting_new_patients
        row.rating = role.rating

        row.cms_pecos_validated = role.cms_pecos_validated
        row.cms_ial2_validated = role.cms_ial2_validated
        row.has_cms_aligned_data_network = role.has_cms_aligned_data_network

        row.location_id = location_id
        row.healthcare_service_id = healthcare_service_id

        session.flush()
        role_id = int(row.id)

    # specialties (normalized SpecialtyRow)
    if role.specialties:
        if is_postgres(session):
            spec_ids = []
            for s in role.specialties:
                stmt = (
                    pg_insert(SpecialtyRow)
                    .values(code=s.code)
                    .on_conflict_do_update(
                        index_elements=[SpecialtyRow.code],
                        set_={"code": s.code},
                    )
                    .returning(SpecialtyRow.id)
                )
                spec_ids.append(int(execute_returning_scalar(session, stmt)))

            join_values = [
                {"practitioner_role_id": role_id, "specialty_id": sid} for sid in spec_ids
            ]
            stmt = (
                pg_insert(PractitionerRoleSpecialtyRow)
                .values(join_values)
                .on_conflict_do_nothing(
                    index_elements=[
                        PractitionerRoleSpecialtyRow.practitioner_role_id,
                        PractitionerRoleSpecialtyRow.specialty_id,
                    ]
                )
            )
            session.execute(stmt)
        else:
            for s in role.specialties:
                srow = session.execute(
                    select(SpecialtyRow).where(SpecialtyRow.code == s.code)
                ).scalar_one_or_none()
                if srow is None:
                    srow = SpecialtyRow(code=s.code)
                    session.add(srow)
                    session.flush()
                join = session.execute(
                    select(PractitionerRoleSpecialtyRow).where(
                        PractitionerRoleSpecialtyRow.practitioner_role_id == role_id,
                        PractitionerRoleSpecialtyRow.specialty_id == int(srow.id),
                    )
                ).scalar_one_or_none()
                if join is None:
                    session.add(
                        PractitionerRoleSpecialtyRow(
                            practitioner_role_id=role_id,
                            specialty_id=int(srow.id),
                        )
                    )

    # telecoms (normalized TelecomRow)
    if role.telecoms:
        if is_postgres(session):
            join_values = []
            for t in role.telecoms:
                telecom_id = get_or_create_telecom_id(session, type=t.type, value=t.value)
                join_values.append({"practitioner_role_id": role_id, "telecom_id": telecom_id})
            stmt = (
                pg_insert(PractitionerRoleTelecomRow)
                .values(join_values)
                .on_conflict_do_nothing(
                    index_elements=[
                        PractitionerRoleTelecomRow.practitioner_role_id,
                        PractitionerRoleTelecomRow.telecom_id,
                    ]
                )
            )
            session.execute(stmt)
        else:
            for t in role.telecoms:
                telecom_id = get_or_create_telecom_id(session, type=t.type, value=t.value)
                join = session.execute(
                    select(PractitionerRoleTelecomRow).where(
                        PractitionerRoleTelecomRow.practitioner_role_id == role_id,
                        PractitionerRoleTelecomRow.telecom_id == telecom_id,
                    )
                ).scalar_one_or_none()
                if join is None:
                    session.add(
                        PractitionerRoleTelecomRow(
                            practitioner_role_id=role_id,
                            telecom_id=telecom_id,
                        )
                    )

    # endpoints
    if role.endpoints:
        if is_postgres(session):
            endpoint_ids = []
            for e in role.endpoints:
                euuid = ensure_uuid(e.resource_uuid)
                stmt = (
                    pg_insert(EndpointRow)
                    .values(
                        resource_uuid=euuid,
                        status="active",
                        connection_type_code="(placeholder)",
                    )
                    .on_conflict_do_update(
                        index_elements=[EndpointRow.resource_uuid],
                        set_={"resource_uuid": euuid},
                    )
                    .returning(EndpointRow.id)
                )
                endpoint_ids.append(int(execute_returning_scalar(session, stmt)))

            join_values = [
                {"practitioner_role_id": role_id, "endpoint_id": eid}
                for eid in endpoint_ids
            ]
            stmt = (
                pg_insert(PractitionerRoleEndpointRow)
                .values(join_values)
                .on_conflict_do_nothing(
                    index_elements=[
                        PractitionerRoleEndpointRow.practitioner_role_id,
                        PractitionerRoleEndpointRow.endpoint_id,
                    ]
                )
            )
            session.execute(stmt)
        else:
            for e in role.endpoints:
                euuid = ensure_uuid(e.resource_uuid)
                erow = session.execute(
                    select(EndpointRow).where(EndpointRow.resource_uuid == euuid)
                ).scalar_one_or_none()
                if erow is None:
                    # Endpoint now has required scalar columns. Use placeholders when
                    # referenced before full Endpoint persistence.
                    erow = EndpointRow(
                        resource_uuid=euuid,
                        status="active",
                        connection_type_code="(placeholder)",
                    )
                    session.add(erow)
                    session.flush()
                join = session.execute(
                    select(PractitionerRoleEndpointRow).where(
                        PractitionerRoleEndpointRow.practitioner_role_id == role_id,
                        PractitionerRoleEndpointRow.endpoint_id == int(erow.id),
                    )
                ).scalar_one_or_none()
                if join is None:
                    session.add(
                        PractitionerRoleEndpointRow(
                            practitioner_role_id=role_id,
                            endpoint_id=int(erow.id),
                        )
                    )

    session.flush()
    return role.model_copy(
        update={
            "id": role_id,
            "practitioner_id": practitioner_id,
            "organization_id": organization_id,
        }
    )


def load_practitioner_role_by_uuid(
    session: Session, resource_uuid: str | uuid.UUID
) -> PractitionerRole:
    ruuid = ensure_uuid(resource_uuid)
    row = session.execute(
        select(PractitionerRoleRow).where(PractitionerRoleRow.resource_uuid == ruuid)
    ).scalar_one()

    pract = session.execute(
        select(PractitionerRow).where(PractitionerRow.id == row.practitioner_id)
    ).scalar_one()
    org = session.execute(
        select(OrganizationRow).where(OrganizationRow.id == row.organization_id)
    ).scalar_one()

    # specialties
    spec_rows = session.execute(
        select(PractitionerRoleSpecialtyRow, SpecialtyRow)
        .where(PractitionerRoleSpecialtyRow.practitioner_role_id == int(row.id))
        .where(PractitionerRoleSpecialtyRow.specialty_id == SpecialtyRow.id)
    ).all()
    specialties = [{"code": str(s.code)} for _, s in spec_rows]

    # telecoms
    from fhir_tablesaw_3tier.db.models import TelecomRow

    tel_rows = session.execute(
        select(PractitionerRoleTelecomRow, TelecomRow)
        .where(PractitionerRoleTelecomRow.practitioner_role_id == int(row.id))
        .where(PractitionerRoleTelecomRow.telecom_id == TelecomRow.id)
    ).all()
    telecoms = [{"type": str(t.type), "value": str(t.value)} for _, t in tel_rows]

    # endpoints
    e_rows = session.execute(
        select(PractitionerRoleEndpointRow, EndpointRow)
        .where(PractitionerRoleEndpointRow.practitioner_role_id == int(row.id))
        .where(PractitionerRoleEndpointRow.endpoint_id == EndpointRow.id)
    ).all()
    endpoints = [{"resource_uuid": str(e.resource_uuid)} for _, e in e_rows]

    location_uuid = None
    if row.location_id is not None:
        loc = session.execute(select(LocationRow).where(LocationRow.id == row.location_id)).scalar_one()
        location_uuid = str(loc.resource_uuid)

    hs_uuid = None
    if row.healthcare_service_id is not None:
        hs = session.execute(
            select(HealthcareServiceRow).where(HealthcareServiceRow.id == row.healthcare_service_id)
        ).scalar_one()
        hs_uuid = str(hs.resource_uuid)

    return PractitionerRole(
        id=int(row.id),
        resource_uuid=str(row.resource_uuid),
        active=row.active,
        practitioner_id=int(row.practitioner_id),
        practitioner_resource_uuid=str(pract.resource_uuid),
        organization_id=int(row.organization_id),
        organization_resource_uuid=str(org.resource_uuid),
        code={
            "system": row.code_system,
            "code": row.code_code,
            "display": row.code_display,
        },
        specialties=specialties,
        telecoms=telecoms,
        endpoints=endpoints,
        accepting_new_patients=row.accepting_new_patients,
        rating=int(row.rating) if row.rating is not None else None,
        cms_pecos_validated=row.cms_pecos_validated,
        cms_ial2_validated=row.cms_ial2_validated,
        has_cms_aligned_data_network=row.has_cms_aligned_data_network,
        location_resource_uuid=location_uuid,
        healthcare_service_resource_uuid=hs_uuid,
    )
