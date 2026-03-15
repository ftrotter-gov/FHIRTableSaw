"""SQLAlchemy models for the 3-tier relational layer.

Constraints from instructions:
- PostgreSQL target
- Bigserial surrogate `id`
- Parallel UUID column `resource_uuid` for FHIR id where applicable
- No Foreign Keys
"""

from __future__ import annotations

import uuid

from sqlalchemy import BigInteger, Boolean, Index, Integer, String, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from fhir_tablesaw_3tier.db.base import Base


# NOTE: SQLite only autoincrements when the PK type is exactly INTEGER.
# We still want bigint semantics for Postgres, so we use a type variant.
BIGINT_PK = BigInteger().with_variant(Integer, "sqlite")


class OrganizationRow(Base):
    """Base Organization registry.

    We maintain a base table for all Organization resources to support
    cross-subtype references (e.g., OrganizationAffiliation can link any subtype).

    Subtype-specific tables (e.g., clinical_organization) may duplicate
    resource_uuid and maintain their own surrogate key, but should also carry
    organization_id to link back to this registry.
    """

    __tablename__ = "organization"

    id: Mapped[int] = mapped_column(BIGINT_PK, primary_key=True, autoincrement=True)
    resource_uuid: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    # e.g., "prov" for ClinicalOrganization; keep generic.
    org_type_code: Mapped[str | None] = mapped_column(String, nullable=True)


Index("ix_organization_resource_uuid", OrganizationRow.resource_uuid, unique=True)


class PractitionerRow(Base):
    __tablename__ = "practitioner"

    id: Mapped[int] = mapped_column(BIGINT_PK, primary_key=True, autoincrement=True)
    resource_uuid: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)

    npi: Mapped[str] = mapped_column(String, nullable=False)
    active_status: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    first_name: Mapped[str | None] = mapped_column(String, nullable=True)
    middle_name: Mapped[str | None] = mapped_column(String, nullable=True)
    last_name: Mapped[str | None] = mapped_column(String, nullable=True)
    prefix: Mapped[str | None] = mapped_column(String, nullable=True)
    non_clinical_suffix: Mapped[str | None] = mapped_column(String, nullable=True)

    other_first_name: Mapped[str | None] = mapped_column(String, nullable=True)
    other_middle_name: Mapped[str | None] = mapped_column(String, nullable=True)
    other_last_name: Mapped[str | None] = mapped_column(String, nullable=True)
    other_prefix: Mapped[str | None] = mapped_column(String, nullable=True)
    other_non_clinical_suffix: Mapped[str | None] = mapped_column(String, nullable=True)

    gender: Mapped[str | None] = mapped_column(String, nullable=True)
    race_code: Mapped[str | None] = mapped_column(String, nullable=True)
    ethnicity_code: Mapped[str | None] = mapped_column(String, nullable=True)

    is_cms_enrolled: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    is_cms_ial2_verified: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    is_participating_in_cms_aligned_data_networks: Mapped[bool | None] = mapped_column(
        Boolean, nullable=True
    )


Index("ix_practitioner_resource_uuid", PractitionerRow.resource_uuid, unique=True)
Index("ix_practitioner_npi", PractitionerRow.npi, unique=True)


class EndpointRow(Base):
    __tablename__ = "endpoint"

    id: Mapped[int] = mapped_column(BIGINT_PK, primary_key=True, autoincrement=True)
    resource_uuid: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    # Minimal representation for now: store raw address and connection info later.
    raw_json: Mapped[str | None] = mapped_column(String, nullable=True)


Index("ix_endpoint_resource_uuid", EndpointRow.resource_uuid, unique=True)


class PractitionerEndpointRow(Base):
    __tablename__ = "practitioner_endpoint"

    id: Mapped[int] = mapped_column(BIGINT_PK, primary_key=True, autoincrement=True)
    practitioner_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    endpoint_id: Mapped[int] = mapped_column(BigInteger, nullable=False)


Index(
    "ix_practitioner_endpoint_pair",
    PractitionerEndpointRow.practitioner_id,
    PractitionerEndpointRow.endpoint_id,
    unique=True,
)


class AddressRow(Base):
    __tablename__ = "address"

    id: Mapped[int] = mapped_column(BIGINT_PK, primary_key=True, autoincrement=True)
    # Address is not a standalone FHIR resource; no resource_uuid.
    line1: Mapped[str | None] = mapped_column(String, nullable=True)
    line2: Mapped[str | None] = mapped_column(String, nullable=True)
    city: Mapped[str | None] = mapped_column(String, nullable=True)
    state: Mapped[str | None] = mapped_column(String, nullable=True)
    postal_code: Mapped[str | None] = mapped_column(String, nullable=True)
    country: Mapped[str | None] = mapped_column(String, nullable=True)


class PractitionerAddressRow(Base):
    __tablename__ = "practitioner_address"

    id: Mapped[int] = mapped_column(BIGINT_PK, primary_key=True, autoincrement=True)
    practitioner_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    address_id: Mapped[int] = mapped_column(BigInteger, nullable=False)


Index(
    "ix_practitioner_address_pair",
    PractitionerAddressRow.practitioner_id,
    PractitionerAddressRow.address_id,
    unique=True,
)


class TelecomRow(Base):
    __tablename__ = "telecom"

    id: Mapped[int] = mapped_column(BIGINT_PK, primary_key=True, autoincrement=True)
    # phone|fax
    type: Mapped[str] = mapped_column(String, nullable=False)
    value: Mapped[str] = mapped_column(String, nullable=False)


Index("ix_telecom_type_value", TelecomRow.type, TelecomRow.value)


class PractitionerTelecomRow(Base):
    __tablename__ = "practitioner_telecom"

    id: Mapped[int] = mapped_column(BIGINT_PK, primary_key=True, autoincrement=True)
    practitioner_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    telecom_id: Mapped[int] = mapped_column(BigInteger, nullable=False)


Index("ix_practitioner_telecom_practitioner", PractitionerTelecomRow.practitioner_id)
Index(
    "ix_practitioner_telecom_pair",
    PractitionerTelecomRow.practitioner_id,
    PractitionerTelecomRow.telecom_id,
    unique=True,
)


class ClinicianTypeRow(Base):
    __tablename__ = "clinician_type"

    id: Mapped[int] = mapped_column(BIGINT_PK, primary_key=True, autoincrement=True)
    # NUCC code
    code: Mapped[str] = mapped_column(String, nullable=False)


Index("ix_clinician_type_code", ClinicianTypeRow.code, unique=True)


class PractitionerClinicianTypeRow(Base):
    __tablename__ = "practitioner_clinician_type"

    id: Mapped[int] = mapped_column(BIGINT_PK, primary_key=True, autoincrement=True)
    practitioner_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    clinician_type_id: Mapped[int] = mapped_column(BigInteger, nullable=False)


Index(
    "ix_practitioner_clinician_type_pair",
    PractitionerClinicianTypeRow.practitioner_id,
    PractitionerClinicianTypeRow.clinician_type_id,
    unique=True,
)


class CredentialRow(Base):
    __tablename__ = "credential"

    id: Mapped[int] = mapped_column(BIGINT_PK, primary_key=True, autoincrement=True)
    value: Mapped[str] = mapped_column(String, nullable=False)


Index("ix_credential_value", CredentialRow.value, unique=True)


class PractitionerCredentialRow(Base):
    __tablename__ = "practitioner_credential"

    id: Mapped[int] = mapped_column(BIGINT_PK, primary_key=True, autoincrement=True)
    practitioner_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    credential_id: Mapped[int] = mapped_column(BigInteger, nullable=False)


Index(
    "ix_practitioner_credential_pair",
    PractitionerCredentialRow.practitioner_id,
    PractitionerCredentialRow.credential_id,
    unique=True,
)


class LanguageProficiencyRow(Base):
    __tablename__ = "language_proficiency"

    id: Mapped[int] = mapped_column(BIGINT_PK, primary_key=True, autoincrement=True)
    practitioner_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    language_code: Mapped[str] = mapped_column(String, nullable=False)
    proficiency_level: Mapped[str | None] = mapped_column(String, nullable=True)


Index("ix_language_proficiency_practitioner", LanguageProficiencyRow.practitioner_id)


class VerificationResultRow(Base):
    __tablename__ = "verification_result"

    id: Mapped[int] = mapped_column(BIGINT_PK, primary_key=True, autoincrement=True)
    resource_uuid: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)

    status: Mapped[str | None] = mapped_column(String, nullable=True)
    attestation_practitioner_uuid: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), nullable=True
    )
    validator_organization_uuid: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), nullable=True
    )

    raw_json: Mapped[str | None] = mapped_column(String, nullable=True)


Index("ix_verification_result_resource_uuid", VerificationResultRow.resource_uuid, unique=True)


class PractitionerVerificationResultRow(Base):
    __tablename__ = "practitioner_verification_result"

    id: Mapped[int] = mapped_column(BIGINT_PK, primary_key=True, autoincrement=True)
    practitioner_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    verification_result_id: Mapped[int] = mapped_column(BigInteger, nullable=False)


Index(
    "ix_practitioner_verification_result_pair",
    PractitionerVerificationResultRow.practitioner_id,
    PractitionerVerificationResultRow.verification_result_id,
    unique=True,
)


# --- PractitionerRole ---


class PractitionerRoleRow(Base):
    __tablename__ = "practitioner_role"

    id: Mapped[int] = mapped_column(BIGINT_PK, primary_key=True, autoincrement=True)
    resource_uuid: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)

    active: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    practitioner_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    organization_id: Mapped[int] = mapped_column(BigInteger, nullable=False)

    # one code concept flattened
    code_system: Mapped[str | None] = mapped_column(String, nullable=True)
    code_code: Mapped[str] = mapped_column(String, nullable=False)
    code_display: Mapped[str | None] = mapped_column(String, nullable=True)

    accepting_new_patients: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    rating: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    cms_pecos_validated: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    cms_ial2_validated: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    has_cms_aligned_data_network: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    location_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    healthcare_service_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)


Index("ix_practitioner_role_resource_uuid", PractitionerRoleRow.resource_uuid, unique=True)
Index("ix_practitioner_role_practitioner", PractitionerRoleRow.practitioner_id)
Index("ix_practitioner_role_organization", PractitionerRoleRow.organization_id)


class PractitionerRoleSpecialtyRow(Base):
    __tablename__ = "practitioner_role_specialty"

    id: Mapped[int] = mapped_column(BIGINT_PK, primary_key=True, autoincrement=True)
    practitioner_role_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    specialty_id: Mapped[int] = mapped_column(BigInteger, nullable=False)


Index(
    "ix_practitioner_role_specialty_pair",
    PractitionerRoleSpecialtyRow.practitioner_role_id,
    PractitionerRoleSpecialtyRow.specialty_id,
    unique=True,
)


class PractitionerRoleTelecomRow(Base):
    __tablename__ = "practitioner_role_telecom"

    id: Mapped[int] = mapped_column(BIGINT_PK, primary_key=True, autoincrement=True)
    practitioner_role_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    telecom_id: Mapped[int] = mapped_column(BigInteger, nullable=False)


Index("ix_practitioner_role_telecom", PractitionerRoleTelecomRow.practitioner_role_id)
Index(
    "ix_practitioner_role_telecom_pair",
    PractitionerRoleTelecomRow.practitioner_role_id,
    PractitionerRoleTelecomRow.telecom_id,
    unique=True,
)


class PractitionerRoleEndpointRow(Base):
    __tablename__ = "practitioner_role_endpoint"

    id: Mapped[int] = mapped_column(BIGINT_PK, primary_key=True, autoincrement=True)
    practitioner_role_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    endpoint_id: Mapped[int] = mapped_column(BigInteger, nullable=False)


Index(
    "ix_practitioner_role_endpoint_pair",
    PractitionerRoleEndpointRow.practitioner_role_id,
    PractitionerRoleEndpointRow.endpoint_id,
    unique=True,
)


class LocationRow(Base):
    __tablename__ = "location"

    id: Mapped[int] = mapped_column(BIGINT_PK, primary_key=True, autoincrement=True)
    resource_uuid: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    raw_json: Mapped[str | None] = mapped_column(String, nullable=True)


Index("ix_location_resource_uuid", LocationRow.resource_uuid, unique=True)


class HealthcareServiceRow(Base):
    __tablename__ = "healthcare_service"

    id: Mapped[int] = mapped_column(BIGINT_PK, primary_key=True, autoincrement=True)
    resource_uuid: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    raw_json: Mapped[str | None] = mapped_column(String, nullable=True)


Index("ix_healthcare_service_resource_uuid", HealthcareServiceRow.resource_uuid, unique=True)


# --- Clinical Organization (subtype-specific table per instruction) ---


class ClinicalOrganizationRow(Base):
    __tablename__ = "clinical_organization"

    id: Mapped[int] = mapped_column(BIGINT_PK, primary_key=True, autoincrement=True)
    resource_uuid: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    organization_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    npi: Mapped[str] = mapped_column(String, nullable=False)
    active: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    name: Mapped[str | None] = mapped_column(String, nullable=True)
    description: Mapped[str | None] = mapped_column(String, nullable=True)

    logo_url: Mapped[str | None] = mapped_column(String, nullable=True)
    rating: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    cms_pecos_validated: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    cms_ial2_validated: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    has_cms_aligned_data_network: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    parent_organization_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    parent_resource_uuid: Mapped[uuid.UUID | None] = mapped_column(Uuid(as_uuid=True), nullable=True)

    # exactly one contact, flattened
    contact_first_name: Mapped[str | None] = mapped_column(String, nullable=True)
    contact_last_name: Mapped[str | None] = mapped_column(String, nullable=True)
    contact_phone: Mapped[str | None] = mapped_column(String, nullable=True)
    contact_fax: Mapped[str | None] = mapped_column(String, nullable=True)
    contact_address_line1: Mapped[str | None] = mapped_column(String, nullable=True)
    contact_address_line2: Mapped[str | None] = mapped_column(String, nullable=True)
    contact_city: Mapped[str | None] = mapped_column(String, nullable=True)
    contact_state: Mapped[str | None] = mapped_column(String, nullable=True)
    contact_postal_code: Mapped[str | None] = mapped_column(String, nullable=True)
    contact_country: Mapped[str | None] = mapped_column(String, nullable=True)


Index("ix_clinical_org_resource_uuid", ClinicalOrganizationRow.resource_uuid, unique=True)
Index("ix_clinical_org_npi", ClinicalOrganizationRow.npi, unique=True)


class ClinicalOrganizationAliasRow(Base):
    __tablename__ = "clinical_organization_alias"

    id: Mapped[int] = mapped_column(BIGINT_PK, primary_key=True, autoincrement=True)
    clinical_organization_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    alias: Mapped[str] = mapped_column(String, nullable=False)
    alias_type: Mapped[str | None] = mapped_column(String, nullable=True)


Index(
    "ix_clinical_org_alias_org",
    ClinicalOrganizationAliasRow.clinical_organization_id,
)


class ClinicalOrganizationTelecomRow(Base):
    __tablename__ = "clinical_organization_telecom"

    id: Mapped[int] = mapped_column(BIGINT_PK, primary_key=True, autoincrement=True)
    clinical_organization_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    telecom_id: Mapped[int] = mapped_column(BigInteger, nullable=False)


Index("ix_clinical_org_telecom_org", ClinicalOrganizationTelecomRow.clinical_organization_id)
Index(
    "ix_clinical_org_telecom_pair",
    ClinicalOrganizationTelecomRow.clinical_organization_id,
    ClinicalOrganizationTelecomRow.telecom_id,
    unique=True,
)


class ClinicalOrganizationAddressRow(Base):
    __tablename__ = "clinical_organization_address"

    id: Mapped[int] = mapped_column(BIGINT_PK, primary_key=True, autoincrement=True)
    clinical_organization_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    address_id: Mapped[int] = mapped_column(BigInteger, nullable=False)


Index(
    "ix_clinical_org_address_pair",
    ClinicalOrganizationAddressRow.clinical_organization_id,
    ClinicalOrganizationAddressRow.address_id,
    unique=True,
)


class ClinicalOrganizationEndpointRow(Base):
    __tablename__ = "clinical_organization_endpoint"

    id: Mapped[int] = mapped_column(BIGINT_PK, primary_key=True, autoincrement=True)
    clinical_organization_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    endpoint_id: Mapped[int] = mapped_column(BigInteger, nullable=False)


Index(
    "ix_clinical_org_endpoint_pair",
    ClinicalOrganizationEndpointRow.clinical_organization_id,
    ClinicalOrganizationEndpointRow.endpoint_id,
    unique=True,
)


# --- OrganizationAffiliation ---


class SpecialtyRow(Base):
    __tablename__ = "specialty"

    id: Mapped[int] = mapped_column(BIGINT_PK, primary_key=True, autoincrement=True)
    # NUCC taxonomy code
    code: Mapped[str] = mapped_column(String, nullable=False)


Index("ix_specialty_code", SpecialtyRow.code, unique=True)


class OrganizationAffiliationRow(Base):
    __tablename__ = "organization_affiliation"

    id: Mapped[int] = mapped_column(BIGINT_PK, primary_key=True, autoincrement=True)
    resource_uuid: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)

    active: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    primary_organization_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    participating_organization_id: Mapped[int] = mapped_column(BigInteger, nullable=False)

    # single role code as flattened strings
    code_system: Mapped[str | None] = mapped_column(String, nullable=True)
    code_code: Mapped[str] = mapped_column(String, nullable=False)
    code_display: Mapped[str | None] = mapped_column(String, nullable=True)


Index("ix_org_affiliation_resource_uuid", OrganizationAffiliationRow.resource_uuid, unique=True)
Index(
    "ix_org_affiliation_org_pair",
    OrganizationAffiliationRow.primary_organization_id,
    OrganizationAffiliationRow.participating_organization_id,
)


class OrganizationAffiliationSpecialtyRow(Base):
    __tablename__ = "organization_affiliation_specialty"

    id: Mapped[int] = mapped_column(BIGINT_PK, primary_key=True, autoincrement=True)
    organization_affiliation_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    specialty_id: Mapped[int] = mapped_column(BigInteger, nullable=False)


Index(
    "ix_org_affiliation_specialty_pair",
    OrganizationAffiliationSpecialtyRow.organization_affiliation_id,
    OrganizationAffiliationSpecialtyRow.specialty_id,
    unique=True,
)


class OrganizationAffiliationTelecomRow(Base):
    __tablename__ = "organization_affiliation_telecom"

    id: Mapped[int] = mapped_column(BIGINT_PK, primary_key=True, autoincrement=True)
    organization_affiliation_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    telecom_id: Mapped[int] = mapped_column(BigInteger, nullable=False)


Index("ix_org_affiliation_telecom", OrganizationAffiliationTelecomRow.organization_affiliation_id)
Index(
    "ix_org_affiliation_telecom_pair",
    OrganizationAffiliationTelecomRow.organization_affiliation_id,
    OrganizationAffiliationTelecomRow.telecom_id,
    unique=True,
)
