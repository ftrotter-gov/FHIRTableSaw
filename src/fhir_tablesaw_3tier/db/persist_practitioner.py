"""Persistence for Practitioner canonical model."""

from __future__ import annotations

import uuid

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from fhir_tablesaw_3tier.db.models import (
    AddressRow,
    ClinicianTypeRow,
    CredentialRow,
    PractitionerAddressRow,
    PractitionerClinicianTypeRow,
    PractitionerCredentialRow,
    PractitionerRow,
    PractitionerTelecomRow,
)
from fhir_tablesaw_3tier.db.persist_common import (
    ensure_uuid,
    get_or_create_address_id,
    get_or_create_telecom_id,
)
from fhir_tablesaw_3tier.db.upsert import execute_returning_scalar, is_postgres
from fhir_tablesaw_3tier.domain.practitioner import Practitioner


def save_practitioner(session: Session, practitioner: Practitioner) -> Practitioner:
    """Upsert-ish save.

    We primarily identify by resource_uuid.
    """

    puuid = ensure_uuid(practitioner.resource_uuid)

    if is_postgres(session):
        stmt = (
            pg_insert(PractitionerRow)
            .values(
                resource_uuid=puuid,
                npi=practitioner.npi,
                active_status=practitioner.active_status,
                first_name=practitioner.first_name,
                middle_name=practitioner.middle_name,
                last_name=practitioner.last_name,
                prefix=practitioner.prefix,
                non_clinical_suffix=practitioner.non_clinical_suffix,
                other_first_name=practitioner.other_first_name,
                other_middle_name=practitioner.other_middle_name,
                other_last_name=practitioner.other_last_name,
                other_prefix=practitioner.other_prefix,
                other_non_clinical_suffix=practitioner.other_non_clinical_suffix,
                gender=practitioner.gender,
                race_code=practitioner.race_code,
                ethnicity_code=practitioner.ethnicity_code,
                is_cms_enrolled=practitioner.is_cms_enrolled,
                is_cms_ial2_verified=practitioner.is_cms_ial2_verified,
                is_participating_in_cms_aligned_data_networks=practitioner.is_participating_in_cms_aligned_data_networks,
            )
            .on_conflict_do_update(
                index_elements=[PractitionerRow.resource_uuid],
                set_={
                    "npi": practitioner.npi,
                    "active_status": practitioner.active_status,
                    "first_name": practitioner.first_name,
                    "middle_name": practitioner.middle_name,
                    "last_name": practitioner.last_name,
                    "prefix": practitioner.prefix,
                    "non_clinical_suffix": practitioner.non_clinical_suffix,
                    "other_first_name": practitioner.other_first_name,
                    "other_middle_name": practitioner.other_middle_name,
                    "other_last_name": practitioner.other_last_name,
                    "other_prefix": practitioner.other_prefix,
                    "other_non_clinical_suffix": practitioner.other_non_clinical_suffix,
                    "gender": practitioner.gender,
                    "race_code": practitioner.race_code,
                    "ethnicity_code": practitioner.ethnicity_code,
                    "is_cms_enrolled": practitioner.is_cms_enrolled,
                    "is_cms_ial2_verified": practitioner.is_cms_ial2_verified,
                    "is_participating_in_cms_aligned_data_networks": practitioner.is_participating_in_cms_aligned_data_networks,
                },
            )
            .returning(PractitionerRow.id)
        )
        practitioner_id = int(execute_returning_scalar(session, stmt))
    else:
        # sqlite/unit-test fallback
        row = session.execute(
            select(PractitionerRow).where(PractitionerRow.resource_uuid == puuid)
        ).scalar_one_or_none()

        if row is None:
            row = PractitionerRow(resource_uuid=puuid, npi=practitioner.npi)
            session.add(row)

        # update scalar columns
        row.npi = practitioner.npi
        row.active_status = practitioner.active_status
        row.first_name = practitioner.first_name
        row.middle_name = practitioner.middle_name
        row.last_name = practitioner.last_name
        row.prefix = practitioner.prefix
        row.non_clinical_suffix = practitioner.non_clinical_suffix

        row.other_first_name = practitioner.other_first_name
        row.other_middle_name = practitioner.other_middle_name
        row.other_last_name = practitioner.other_last_name
        row.other_prefix = practitioner.other_prefix
        row.other_non_clinical_suffix = practitioner.other_non_clinical_suffix

        row.gender = practitioner.gender
        row.race_code = practitioner.race_code
        row.ethnicity_code = practitioner.ethnicity_code

        row.is_cms_enrolled = practitioner.is_cms_enrolled
        row.is_cms_ial2_verified = practitioner.is_cms_ial2_verified
        row.is_participating_in_cms_aligned_data_networks = (
            practitioner.is_participating_in_cms_aligned_data_networks
        )

        session.flush()
        practitioner_id = int(row.id)

    # addresses: upsert/dedupe + join-table insert-ignore
    if practitioner.addresses:
        if is_postgres(session):
            join_values = []
            for a in practitioner.addresses:
                address_id = get_or_create_address_id(
                    session,
                    line1=a.line1,
                    line2=a.line2,
                    city=a.city,
                    state=a.state,
                    postal_code=a.postal_code,
                    country=a.country,
                )
                join_values.append({"practitioner_id": practitioner_id, "address_id": address_id})
            if join_values:
                stmt = (
                    pg_insert(PractitionerAddressRow)
                    .values(join_values)
                    .on_conflict_do_nothing(
                        index_elements=[
                            PractitionerAddressRow.practitioner_id,
                            PractitionerAddressRow.address_id,
                        ]
                    )
                )
                session.execute(stmt)
        else:
            for a in practitioner.addresses:
                addr_row = session.execute(
                    select(AddressRow).where(
                        AddressRow.line1 == a.line1,
                        AddressRow.line2 == a.line2,
                        AddressRow.city == a.city,
                        AddressRow.state == a.state,
                        AddressRow.postal_code == a.postal_code,
                        AddressRow.country == a.country,
                    )
                ).scalar_one_or_none()
                if addr_row is None:
                    addr_row = AddressRow(
                        line1=a.line1,
                        line2=a.line2,
                        city=a.city,
                        state=a.state,
                        postal_code=a.postal_code,
                        country=a.country,
                    )
                    session.add(addr_row)
                    session.flush()

                # join
                join = session.execute(
                    select(PractitionerAddressRow).where(
                        PractitionerAddressRow.practitioner_id == practitioner_id,
                        PractitionerAddressRow.address_id == int(addr_row.id),
                    )
                ).scalar_one_or_none()
                if join is None:
                    session.add(
                        PractitionerAddressRow(
                            practitioner_id=practitioner_id,
                            address_id=int(addr_row.id),
                        )
                    )

    # telecoms: normalized telecom + join table
    if practitioner.telecoms:
        if is_postgres(session):
            join_values = []
            for t in practitioner.telecoms:
                telecom_id = get_or_create_telecom_id(session, type=t.type, value=t.value)
                join_values.append({"practitioner_id": practitioner_id, "telecom_id": telecom_id})
            stmt = (
                pg_insert(PractitionerTelecomRow)
                .values(join_values)
                .on_conflict_do_nothing(
                    index_elements=[
                        PractitionerTelecomRow.practitioner_id,
                        PractitionerTelecomRow.telecom_id,
                    ]
                )
            )
            session.execute(stmt)
        else:
            for t in practitioner.telecoms:
                telecom_id = get_or_create_telecom_id(session, type=t.type, value=t.value)
                join = session.execute(
                    select(PractitionerTelecomRow).where(
                        PractitionerTelecomRow.practitioner_id == practitioner_id,
                        PractitionerTelecomRow.telecom_id == telecom_id,
                    )
                ).scalar_one_or_none()
                if join is None:
                    session.add(
                        PractitionerTelecomRow(
                            practitioner_id=practitioner_id, telecom_id=telecom_id
                        )
                    )

    # clinician types: normalized + join
    if practitioner.clinician_types:
        if is_postgres(session):
            # Upsert normalized types, then join insert-ignore.
            type_ids = []
            for ct in practitioner.clinician_types:
                stmt = (
                    pg_insert(ClinicianTypeRow)
                    .values(code=ct.code)
                    .on_conflict_do_update(
                        index_elements=[ClinicianTypeRow.code],
                        set_={"code": ct.code},
                    )
                    .returning(ClinicianTypeRow.id)
                )
                type_ids.append(int(execute_returning_scalar(session, stmt)))

            join_values = [
                {"practitioner_id": practitioner_id, "clinician_type_id": cid}
                for cid in type_ids
            ]
            stmt = (
                pg_insert(PractitionerClinicianTypeRow)
                .values(join_values)
                .on_conflict_do_nothing(
                    index_elements=[
                        PractitionerClinicianTypeRow.practitioner_id,
                        PractitionerClinicianTypeRow.clinician_type_id,
                    ]
                )
            )
            session.execute(stmt)
        else:
            for ct in practitioner.clinician_types:
                ct_row = session.execute(
                    select(ClinicianTypeRow).where(ClinicianTypeRow.code == ct.code)
                ).scalar_one_or_none()
                if ct_row is None:
                    ct_row = ClinicianTypeRow(code=ct.code)
                    session.add(ct_row)
                    session.flush()
                join = session.execute(
                    select(PractitionerClinicianTypeRow).where(
                        PractitionerClinicianTypeRow.practitioner_id == practitioner_id,
                        PractitionerClinicianTypeRow.clinician_type_id == int(ct_row.id),
                    )
                ).scalar_one_or_none()
                if join is None:
                    session.add(
                        PractitionerClinicianTypeRow(
                            practitioner_id=practitioner_id,
                            clinician_type_id=int(ct_row.id),
                        )
                    )

    # credentials: normalized + join
    if practitioner.credentials:
        if is_postgres(session):
            cred_ids = []
            for c in practitioner.credentials:
                stmt = (
                    pg_insert(CredentialRow)
                    .values(value=c.value)
                    .on_conflict_do_update(
                        index_elements=[CredentialRow.value],
                        set_={"value": c.value},
                    )
                    .returning(CredentialRow.id)
                )
                cred_ids.append(int(execute_returning_scalar(session, stmt)))

            join_values = [
                {"practitioner_id": practitioner_id, "credential_id": cid}
                for cid in cred_ids
            ]
            stmt = (
                pg_insert(PractitionerCredentialRow)
                .values(join_values)
                .on_conflict_do_nothing(
                    index_elements=[
                        PractitionerCredentialRow.practitioner_id,
                        PractitionerCredentialRow.credential_id,
                    ]
                )
            )
            session.execute(stmt)
        else:
            for c in practitioner.credentials:
                c_row = session.execute(
                    select(CredentialRow).where(CredentialRow.value == c.value)
                ).scalar_one_or_none()
                if c_row is None:
                    c_row = CredentialRow(value=c.value)
                    session.add(c_row)
                    session.flush()
                join = session.execute(
                    select(PractitionerCredentialRow).where(
                        PractitionerCredentialRow.practitioner_id == practitioner_id,
                        PractitionerCredentialRow.credential_id == int(c_row.id),
                    )
                ).scalar_one_or_none()
                if join is None:
                    session.add(
                        PractitionerCredentialRow(
                            practitioner_id=practitioner_id,
                            credential_id=int(c_row.id),
                        )
                    )

    session.flush()
    return practitioner.model_copy(update={"id": practitioner_id})


def load_practitioner_by_uuid(session: Session, resource_uuid: str | uuid.UUID) -> Practitioner:
    puuid = ensure_uuid(resource_uuid)
    row = session.execute(
        select(PractitionerRow).where(PractitionerRow.resource_uuid == puuid)
    ).scalar_one()

    # We keep load minimal for now (enough for round-trip smoke tests).
    return Practitioner(
        id=int(row.id),
        resource_uuid=str(row.resource_uuid),
        npi=row.npi,
        active_status=row.active_status,
        first_name=row.first_name,
        middle_name=row.middle_name,
        last_name=row.last_name,
        prefix=row.prefix,
        non_clinical_suffix=row.non_clinical_suffix,
        other_first_name=row.other_first_name,
        other_middle_name=row.other_middle_name,
        other_last_name=row.other_last_name,
        other_prefix=row.other_prefix,
        other_non_clinical_suffix=row.other_non_clinical_suffix,
        gender=row.gender,
        race_code=row.race_code,
        ethnicity_code=row.ethnicity_code,
        is_cms_enrolled=row.is_cms_enrolled,
        is_cms_ial2_verified=row.is_cms_ial2_verified,
        is_participating_in_cms_aligned_data_networks=row.is_participating_in_cms_aligned_data_networks,
        # Relationships intentionally omitted for now.
    )
