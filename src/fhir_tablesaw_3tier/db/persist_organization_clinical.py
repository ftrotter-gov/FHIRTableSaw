"""Persistence for ClinicalOrganization canonical model."""

from __future__ import annotations

import uuid

from sqlalchemy import select
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
from fhir_tablesaw_3tier.domain.organization_clinical import ClinicalOrganization


def save_clinical_organization(
    session: Session, org: ClinicalOrganization
) -> ClinicalOrganization:
    ouuid = ensure_uuid(org.resource_uuid)

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
    row.parent_resource_uuid = ensure_uuid(org.part_of_resource_uuid) if org.part_of_resource_uuid else None

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

    # addresses: naive exact dedupe + join
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
