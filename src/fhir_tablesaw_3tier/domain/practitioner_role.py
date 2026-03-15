from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from fhir_tablesaw_3tier.domain.common import CanonicalBase


class RoleTelecom(CanonicalBase):
    type: Literal["phone", "fax"]
    value: str


class RoleCode(CanonicalBase):
    system: str | None = None
    code: str
    display: str | None = None


class Specialty(CanonicalBase):
    # NUCC taxonomy code
    code: str


class EndpointRef(CanonicalBase):
    resource_uuid: str


class PractitionerRole(CanonicalBase):
    """Canonical PractitionerRole.

    Bridge between a Practitioner and an Organization.

    - `id` is the internal relational surrogate key (bigint).
    - `resource_uuid` is the FHIR PractitionerRole.id (UUID string).
    """

    model_config = ConfigDict(extra="forbid")

    id: int | None = None
    resource_uuid: str

    active: bool | None = None

    practitioner_id: int | None = None
    practitioner_resource_uuid: str

    organization_id: int | None = None
    organization_resource_uuid: str

    code: RoleCode
    specialties: list[Specialty] = Field(default_factory=list)
    telecoms: list[RoleTelecom] = Field(default_factory=list)
    endpoints: list[EndpointRef] = Field(default_factory=list)

    # extensions
    accepting_new_patients: bool | None = None
    rating: int | None = None

    cms_pecos_validated: bool | None = None
    cms_ial2_validated: bool | None = None
    has_cms_aligned_data_network: bool | None = None

    # service context: CMS-constrained 0..1 each
    location_resource_uuid: str | None = None
    healthcare_service_resource_uuid: str | None = None


class DroppedRepeatsReport(BaseModel):
    model_config = ConfigDict(extra="forbid")

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
