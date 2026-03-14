from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from fhir_tablesaw_3tier.domain.common import CanonicalBase
from fhir_tablesaw_3tier.domain.practitioner import Address as CanonicalAddress


class OrgTelecom(CanonicalBase):
    type: Literal["phone", "fax"]
    value: str


class OrgAlias(CanonicalBase):
    alias: str
    alias_type: str | None = None


class OrgContact(CanonicalBase):
    first_name: str | None = None
    last_name: str | None = None
    phone: str | None = None
    fax: str | None = None

    # flattened address
    address_line1: str | None = None
    address_line2: str | None = None
    city: str | None = None
    state: str | None = None
    postal_code: str | None = None
    country: str | None = None


class EndpointRef(CanonicalBase):
    resource_uuid: str


class ClinicalOrganization(CanonicalBase):
    """Canonical ClinicalOrganization.

    Canonical `id` is the FHIR resource id (UUID string).
    """

    model_config = ConfigDict(extra="forbid")

    id: str
    npi: str

    active: bool | None = None
    name: str | None = None
    description: str | None = None

    # logo URL
    logo_url: str | None = None

    # exactly one rating
    rating: int | None = None

    # CMS verification booleans
    cms_pecos_validated: bool | None = None
    cms_ial2_validated: bool | None = None
    has_cms_aligned_data_network: bool | None = None

    aliases: list[OrgAlias] = Field(default_factory=list)
    telecoms: list[OrgTelecom] = Field(default_factory=list)

    # many addresses via normalized address + join
    addresses: list[CanonicalAddress] = Field(default_factory=list)

    part_of_organization_id: int | None = None
    part_of_resource_uuid: str | None = None

    contact: OrgContact | None = None
    endpoints: list[EndpointRef] = Field(default_factory=list)


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
