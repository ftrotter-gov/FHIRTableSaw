from __future__ import annotations

from typing import Any, Literal

from pydantic import ConfigDict, Field

from fhir_tablesaw_3tier.domain.common import CanonicalBase


class LocationTelecom(CanonicalBase):
    type: Literal["phone", "fax"]
    value: str


class LocationPosition(CanonicalBase):
    latitude: float
    longitude: float
    altitude: float | None = None


class LocationBoundaryGeoJson(CanonicalBase):
    """Canonical representation of the location-boundary-geojson extension.

    The FHIR extension uses valueAttachment. We support either:
    - Attachment.data (base64) or
    - Attachment.url (external)

    For relational compression, we store a single text field (either the decoded
    JSON string if we can decode, or a placeholder string like "url:<...>" if
    URL-only). See FHIR layer for exact rules.
    """

    geojson_text: str


class LocationAccessibility(CanonicalBase):
    system: str | None = None
    code: str
    display: str | None = None


class LocationHoursOfOperation(CanonicalBase):
    all_day: bool | None = None
    opening_time: str | None = None
    closing_time: str | None = None


class Location(CanonicalBase):
    """Canonical NDH Location.

    - `id` is the internal relational surrogate key.
    - `resource_uuid` is the FHIR Location.id (UUID string).
    """

    model_config = ConfigDict(extra="forbid")

    id: int | None = None
    resource_uuid: str

    status: str  # required (active | suspended | inactive)
    name: str

    description: str | None = None
    availability_exceptions: str | None = None

    managing_organization_resource_uuid: str | None = None
    part_of_location_resource_uuid: str | None = None

    # Address is 0..1 in NDH snapshot, but Location.md says one-and-only-one.
    # We accept optional but will warn when absent.
    address_line1: str | None = None
    address_line2: str | None = None
    address_city: str | None = None
    address_state: str | None = None
    address_postal_code: str | None = None
    address_country: str | None = None

    position: LocationPosition | None = None

    boundary_geojson: LocationBoundaryGeoJson | None = None
    accessibility: list[LocationAccessibility] = Field(default_factory=list)

    telecoms: list[LocationTelecom] = Field(default_factory=list)
    endpoints: list[str] = Field(default_factory=list)  # Endpoint.resource_uuid strings
    hours_of_operation: list[LocationHoursOfOperation] = Field(default_factory=list)

    # NDH newpatients and verification-status are slice extensions. We keep them
    # as overflow for now; can be normalized later.
    newpatients: list[dict[str, Any]] = Field(default_factory=list)
    verification_status: dict[str, Any] | None = None
