"""Persistence for ClinicalOrganization canonical model."""

from __future__ import annotations

import uuid

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from fhir_tablesaw_3tier.db.models import (
    AddressRow,
    ClinicalOrganizationAddressRow,
    ClinicalOrganizationAliasRow,
    ClinicalOrganizationEndpointRow,
    ClinicalOrganizationRow,
    ClinicalOrganizationTelecomRow,
    EndpointRow,
    OrganizationRow,
)
from fhir_tablesaw_3tier.db.persist_common import ensure_uuid, get_or_create_telecom_id
from fhir_tablesaw_3tier.db.persist_common import get_or_create_address_id
from fhir_tablesaw_3tier.db.upsert import execute_returning_scalar, is_postgres
from fhir_tablesaw_3tier.domain.organization_clinical import ClinicalOrganization


def save_clinical_organization(
    session: Session, org: ClinicalOrganization
) -> ClinicalOrganization:
    ouuid = ensure_uuid(org.resource_uuid)

    if is_postgres(session):
        # Base org registry row (keep org_type_code aligned to prov).
        base_stmt = (
            pg_insert(OrganizationRow)
            .values(resource_uuid=ouuid, org_type_code="prov")
            .on_conflict_do_update(
                index_elements=[OrganizationRow.resource_uuid],
                set_={"org_type_code": "prov"},
            )
            .returning(OrganizationRow.id)
        )
        base_id = int(execute_returning_scalar(session, base_stmt))

        stmt = (
            pg_insert(ClinicalOrganizationRow)
            .values(
                resource_uuid=ouuid,
                organization_id=base_id,
                npi=org.npi,
                active=org.active,
                name=org.name,
                description=org.description,
                logo_url=org.logo_url,
                rating=org.rating,
                cms_pecos_validated=org.cms_pecos_validated,
                cms_ial2_validated=org.cms_ial2_validated,
                has_cms_aligned_data_network=org.has_cms_aligned_data_network,
                parent_organization_id=org.part_of_organization_id,
                parent_resource_uuid=ensure_uuid(org.part_of_resource_uuid)
                if org.part_of_resource_uuid
                else None,
                contact_first_name=None if org.contact is None else org.contact.first_name,
                contact_last_name=None if org.contact is None else org.contact.last_name,
                contact_phone=None if org.contact is None else org.contact.phone,
                contact_fax=None if org.contact is None else org.contact.fax,
                contact_address_line1=None if org.contact is None else org.contact.address_line1,
                contact_address_line2=None if org.contact is None else org.contact.address_line2,
                contact_city=None if org.contact is None else org.contact.city,
                contact_state=None if org.contact is None else org.contact.state,
                contact_postal_code=None if org.contact is None else org.contact.postal_code,
                contact_country=None if org.contact is None else org.contact.country,
            )
            .on_conflict_do_update(
                index_elements=[ClinicalOrganizationRow.resource_uuid],
                set_={
                    "organization_id": base_id,
                    "npi": org.npi,
                    "active": org.active,
                    "name": org.name,
                    "description": org.description,
                    "logo_url": org.logo_url,
                    "rating": org.rating,
                    "cms_pecos_validated": org.cms_pecos_validated,
                    "cms_ial2_validated": org.cms_ial2_validated,
                    "has_cms_aligned_data_network": org.has_cms_aligned_data_network,
                    "parent_organization_id": org.part_of_organization_id,
                    "parent_resource_uuid": ensure_uuid(org.part_of_resource_uuid)
                    if org.part_of_resource_uuid
                    else None,
                    "contact_first_name": None if org.contact is None else org.contact.first_name,
                    "contact_last_name": None if org.contact is None else org.contact.last_name,
                    "contact_phone": None if org.contact is None else org.contact.phone,
                    "contact_fax": None if org.contact is None else org.contact.fax,
                    "contact_address_line1": None if org.contact is None else org.contact.address_line1,
                    "contact_address_line2": None if org.contact is None else org.contact.address_line2,
                    "contact_city": None if org.contact is None else org.contact.city,
                    "contact_state": None if org.contact is None else org.contact.state,
                    "contact_postal_code": None if org.contact is None else org.contact.postal_code,
                    "contact_country": None if org.contact is None else org.contact.country,
                },
            )
            .returning(ClinicalOrganizationRow.id)
        )
        org_id = int(execute_returning_scalar(session, stmt))
    else:
        # sqlite fallback
        # ensure base organization registry row
        base = session.execute(
            select(OrganizationRow).where(OrganizationRow.resource_uuid == ouuid)
        ).scalar_one_or_none()
        if base is None:
            base = OrganizationRow(resource_uuid=ouuid, org_type_code="prov")
            session.add(base)
            session.flush()
        else:
            # keep type code aligned
            if base.org_type_code is None:
                base.org_type_code = "prov"

        row = session.execute(
            select(ClinicalOrganizationRow).where(ClinicalOrganizationRow.resource_uuid == ouuid)
        ).scalar_one_or_none()
        if row is None:
            row = ClinicalOrganizationRow(resource_uuid=ouuid, npi=org.npi)
            session.add(row)

        row.organization_id = int(base.id)
        row.npi = org.npi
        row.active = org.active
        row.name = org.name
        row.description = org.description
        row.logo_url = org.logo_url
        row.rating = org.rating
        row.cms_pecos_validated = org.cms_pecos_validated
        row.cms_ial2_validated = org.cms_ial2_validated
        row.has_cms_aligned_data_network = org.has_cms_aligned_data_network

        row.parent_organization_id = org.part_of_organization_id
        row.parent_resource_uuid = (
            ensure_uuid(org.part_of_resource_uuid) if org.part_of_resource_uuid else None
        )

        # flattened contact
        if org.contact is None:
            row.contact_first_name = None
            row.contact_last_name = None
            row.contact_phone = None
            row.contact_fax = None
            row.contact_address_line1 = None
            row.contact_address_line2 = None
            row.contact_city = None
            row.contact_state = None
            row.contact_postal_code = None
            row.contact_country = None
        else:
            row.contact_first_name = org.contact.first_name
            row.contact_last_name = org.contact.last_name
            row.contact_phone = org.contact.phone
            row.contact_fax = org.contact.fax
            row.contact_address_line1 = org.contact.address_line1
            row.contact_address_line2 = org.contact.address_line2
            row.contact_city = org.contact.city
            row.contact_state = org.contact.state
            row.contact_postal_code = org.contact.postal_code
            row.contact_country = org.contact.country

        session.flush()
        org_id = int(row.id)

    # aliases
    if org.aliases:
        if is_postgres(session):
            values = [
                {
                    "clinical_organization_id": org_id,
                    "alias": a.alias,
                    "alias_type": a.alias_type,
                }
                for a in org.aliases
            ]
            stmt = (
                pg_insert(ClinicalOrganizationAliasRow)
                .values(values)
                .on_conflict_do_nothing(
                    index_elements=[
                        ClinicalOrganizationAliasRow.clinical_organization_id,
                        ClinicalOrganizationAliasRow.alias,
                        ClinicalOrganizationAliasRow.alias_type,
                    ]
                )
            )
            session.execute(stmt)
        else:
            for a in org.aliases:
                existing = session.execute(
                    select(ClinicalOrganizationAliasRow).where(
                        ClinicalOrganizationAliasRow.clinical_organization_id == org_id,
                        ClinicalOrganizationAliasRow.alias == a.alias,
                        ClinicalOrganizationAliasRow.alias_type == a.alias_type,
                    )
                ).scalar_one_or_none()
                if existing is None:
                    session.add(
                        ClinicalOrganizationAliasRow(
                            clinical_organization_id=org_id,
                            alias=a.alias,
                            alias_type=a.alias_type,
                        )
                    )

    # telecoms (normalized)
    if org.telecoms:
        if is_postgres(session):
            join_values = []
            for t in org.telecoms:
                telecom_id = get_or_create_telecom_id(session, type=t.type, value=t.value)
                join_values.append({"clinical_organization_id": org_id, "telecom_id": telecom_id})
            stmt = (
                pg_insert(ClinicalOrganizationTelecomRow)
                .values(join_values)
                .on_conflict_do_nothing(
                    index_elements=[
                        ClinicalOrganizationTelecomRow.clinical_organization_id,
                        ClinicalOrganizationTelecomRow.telecom_id,
                    ]
                )
            )
            session.execute(stmt)
        else:
            for t in org.telecoms:
                telecom_id = get_or_create_telecom_id(session, type=t.type, value=t.value)
                join = session.execute(
                    select(ClinicalOrganizationTelecomRow).where(
                        ClinicalOrganizationTelecomRow.clinical_organization_id == org_id,
                        ClinicalOrganizationTelecomRow.telecom_id == telecom_id,
                    )
                ).scalar_one_or_none()
                if join is None:
                    session.add(
                        ClinicalOrganizationTelecomRow(
                            clinical_organization_id=org_id,
                            telecom_id=telecom_id,
                        )
                    )

    # addresses: upsert/dedupe + join
    if org.addresses:
        if is_postgres(session):
            join_values = []
            for a in org.addresses:
                address_id = get_or_create_address_id(
                    session,
                    line1=a.line1,
                    line2=a.line2,
                    city=a.city,
                    state=a.state,
                    postal_code=a.postal_code,
                    country=a.country,
                )
                join_values.append({"clinical_organization_id": org_id, "address_id": address_id})
            stmt = (
                pg_insert(ClinicalOrganizationAddressRow)
                .values(join_values)
                .on_conflict_do_nothing(
                    index_elements=[
                        ClinicalOrganizationAddressRow.clinical_organization_id,
                        ClinicalOrganizationAddressRow.address_id,
                    ]
                )
            )
            session.execute(stmt)
        else:
            for a in org.addresses:
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

                join = session.execute(
                    select(ClinicalOrganizationAddressRow).where(
                        ClinicalOrganizationAddressRow.clinical_organization_id == org_id,
                        ClinicalOrganizationAddressRow.address_id == int(addr_row.id),
                    )
                ).scalar_one_or_none()
                if join is None:
                    session.add(
                        ClinicalOrganizationAddressRow(
                            clinical_organization_id=org_id,
                            address_id=int(addr_row.id),
                        )
                    )

    # endpoints: ensure endpoint rows exist + join
    if org.endpoints:
        if is_postgres(session):
            endpoint_ids: list[int] = []
            for e in org.endpoints:
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
                {"clinical_organization_id": org_id, "endpoint_id": eid}
                for eid in endpoint_ids
            ]
            stmt = (
                pg_insert(ClinicalOrganizationEndpointRow)
                .values(join_values)
                .on_conflict_do_nothing(
                    index_elements=[
                        ClinicalOrganizationEndpointRow.clinical_organization_id,
                        ClinicalOrganizationEndpointRow.endpoint_id,
                    ]
                )
            )
            session.execute(stmt)
        else:
            for e in org.endpoints:
                euuid = ensure_uuid(e.resource_uuid)
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
                join = session.execute(
                    select(ClinicalOrganizationEndpointRow).where(
                        ClinicalOrganizationEndpointRow.clinical_organization_id == org_id,
                        ClinicalOrganizationEndpointRow.endpoint_id == int(erow.id),
                    )
                ).scalar_one_or_none()
                if join is None:
                    session.add(
                        ClinicalOrganizationEndpointRow(
                            clinical_organization_id=org_id,
                            endpoint_id=int(erow.id),
                        )
                    )

    session.flush()
    return org.model_copy(update={"id": org_id})


def load_clinical_organization_by_uuid(
    session: Session, resource_uuid: str | uuid.UUID
) -> ClinicalOrganization:
    ouuid = ensure_uuid(resource_uuid)
    row = session.execute(
        select(ClinicalOrganizationRow).where(ClinicalOrganizationRow.resource_uuid == ouuid)
    ).scalar_one()

    # Minimal load (enough for round-trip smoke).
    return ClinicalOrganization(
        id=int(row.id),
        resource_uuid=str(row.resource_uuid),
        npi=row.npi,
        active=row.active,
        name=row.name,
        description=row.description,
        logo_url=row.logo_url,
        rating=int(row.rating) if row.rating is not None else None,
        cms_pecos_validated=row.cms_pecos_validated,
        cms_ial2_validated=row.cms_ial2_validated,
        has_cms_aligned_data_network=row.has_cms_aligned_data_network,
        part_of_organization_id=row.parent_organization_id,
        part_of_resource_uuid=str(row.parent_resource_uuid) if row.parent_resource_uuid else None,
        # relationships omitted for now
    )
