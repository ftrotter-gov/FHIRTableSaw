from __future__ import annotations

from pydantic import ConfigDict, Field

from fhir_tablesaw_3tier.domain.common import CanonicalBase


class EndpointCoding(CanonicalBase):
    system: str | None = None
    code: str
    display: str | None = None


class EndpointPayloadType(CanonicalBase):
    """Flattened representation of a payload type concept.

    We store one coding per payload type entry.
    """

    system: str | None = None
    code: str
    display: str | None = None


class Endpoint(CanonicalBase):
    """Canonical NDH Endpoint.

    This implements exactly the in-scope fields from AI_Instructions/Endpoint.md.
    """

    model_config = ConfigDict(extra="forbid")

    id: int | None = None
    resource_uuid: str

    status: str
    connection_type: EndpointCoding
    name: str | None = None
    endpoint_rank: int | None = None

    # Allow empty (do not enforce >=1 per user instruction)
    payload_types: list[EndpointPayloadType] = Field(default_factory=list)
