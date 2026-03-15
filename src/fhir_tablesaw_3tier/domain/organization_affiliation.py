from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from fhir_tablesaw_3tier.domain.common import CanonicalBase


class AffiliationTelecom(CanonicalBase):
    type: Literal["phone", "fax"]
    value: str


class AffiliationCode(CanonicalBase):
    system: str | None = None
    code: str
    display: str | None = None


class Specialty(CanonicalBase):
    # NUCC taxonomy code string
    code: str


class OrganizationAffiliation(CanonicalBase):
    """Canonical OrganizationAffiliation.

    Bridge between two organizations.
    - `id` is internal relational surrogate key.
    - `resource_uuid` is the FHIR OrganizationAffiliation.id (UUID string).

    Both sides of the bridge are required in CMS practice.
    """

    model_config = ConfigDict(extra="forbid")

    id: int | None = None
    resource_uuid: str

    active: bool | None = None

    primary_organization_id: int | None = None
    primary_organization_resource_uuid: str

    participating_organization_id: int | None = None
    participating_organization_resource_uuid: str

    code: AffiliationCode
    specialties: list[Specialty] = Field(default_factory=list)
    telecoms: list[AffiliationTelecom] = Field(default_factory=list)

    # NDH allows endpoints; Endpoint.md says OrganizationAffiliation is an allowed
    # link target. We store only references here.
    endpoints: list[dict] = Field(default_factory=list)


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
