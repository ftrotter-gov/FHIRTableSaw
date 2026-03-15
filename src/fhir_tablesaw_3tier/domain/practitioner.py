from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from fhir_tablesaw_3tier.domain.common import CanonicalBase


class Telecom(CanonicalBase):
    type: Literal["phone", "fax"]
    value: str


class Address(CanonicalBase):
    line1: str | None = None
    line2: str | None = None
    city: str | None = None
    state: str | None = None
    postal_code: str | None = None
    country: str | None = None


class ClinicianType(CanonicalBase):
    code: str


class Credential(CanonicalBase):
    value: str


class LanguageProficiency(CanonicalBase):
    language_code: str
    proficiency_level: str | None = None


class EndpointRef(CanonicalBase):
    # Minimal: just the resource UUID for now.
    resource_uuid: str


class VerificationResultRef(CanonicalBase):
    resource_uuid: str
    status: str | None = None
    attestation_practitioner_uuid: str | None = None
    validator_organization_uuid: str | None = None


class Practitioner(CanonicalBase):
    """Canonical Practitioner.

    `id` is the internal relational surrogate key (bigint).
    `resource_uuid` is the FHIR resource id (UUID string).

    Note: When parsing from FHIR, `id` will typically be None until the object
    is persisted.
    """

    id: int | None = None
    resource_uuid: str
    npi: str
    active_status: bool | None = None

    first_name: str | None = None
    middle_name: str | None = None
    last_name: str | None = None
    prefix: str | None = None
    non_clinical_suffix: str | None = None

    other_first_name: str | None = None
    other_middle_name: str | None = None
    other_last_name: str | None = None
    other_prefix: str | None = None
    other_non_clinical_suffix: str | None = None

    gender: str | None = None
    race_code: str | None = None
    ethnicity_code: str | None = None

    is_cms_enrolled: bool | None = None
    is_cms_ial2_verified: bool | None = None
    is_participating_in_cms_aligned_data_networks: bool | None = None

    endpoints: list[EndpointRef] = Field(default_factory=list)
    verification_results: list[VerificationResultRef] = Field(default_factory=list)
    addresses: list[Address] = Field(default_factory=list)
    clinician_types: list[ClinicianType] = Field(default_factory=list)
    credentials: list[Credential] = Field(default_factory=list)
    telecoms: list[Telecom] = Field(default_factory=list)
    language_proficiencies: list[LanguageProficiency] = Field(default_factory=list)


class DroppedRepeatsReport(BaseModel):
    model_config = ConfigDict(extra="forbid")

    # key = dotted path, value = count
    dropped_counts: dict[str, int] = Field(default_factory=dict)

    def add(self, path: str, count: int) -> None:
        self.dropped_counts[path] = self.dropped_counts.get(path, 0) + count

    def to_text(self) -> str:
        if not self.dropped_counts:
            return "(none)"

        lines = []
        for k, v in sorted(self.dropped_counts.items(), key=lambda kv: (-kv[1], kv[0])):
            lines.append(f"{k}: {v}")
        return "\n".join(lines)
