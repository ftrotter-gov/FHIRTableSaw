"""Persistence for Practitioner canonical model."""

from __future__ import annotations

import uuid

from sqlalchemy import select
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
from fhir_tablesaw_3tier.db.persist_common import ensure_uuid, get_or_create_telecom_id
from fhir_tablesaw_3tier.domain.practitioner import Practitioner


def save_practitioner(session: Session, practitioner: Practitioner) -> Practitioner:
    """Upsert-ish save.

    We primarily identify by resource_uuid.
    """

    puuid = ensure_uuid(practitioner.resource_uuid)
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

    # addresses: naive insert-only + join (dedupe by exact match)
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
                PractitionerTelecomRow(practitioner_id=practitioner_id, telecom_id=telecom_id)
            )

    # clinician types: normalized + join
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
