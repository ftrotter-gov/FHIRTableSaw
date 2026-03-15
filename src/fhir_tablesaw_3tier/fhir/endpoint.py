from __future__ import annotations

from typing import Any

import uuid

from fhir_tablesaw_3tier.domain.dropped_repeats import DroppedRepeatsReport
from fhir_tablesaw_3tier.domain.endpoint import Endpoint, EndpointCoding, EndpointPayloadType
from fhir_tablesaw_3tier.fhir.constants import NDH_ENDPOINT_RANK_EXT_URL
from fhir_tablesaw_3tier.fhir.r4_models import (
    CodeableConcept,
    Coding,
    EndpointResource,
    Extension,
)


def _warn(msg: str) -> None:
    print(f"WARNING: {msg}")


def endpoint_from_fhir_json(
    raw: dict[str, Any], *, fhir_server_url: str | None = None
) -> tuple[Endpoint, DroppedRepeatsReport]:
    """Parse FHIR Endpoint JSON into canonical Endpoint."""

    _ = fhir_server_url
    report = DroppedRepeatsReport()
    fhir = EndpointResource.model_validate(raw)

    # resource_uuid
    if fhir.id:
        resource_uuid = str(fhir.id)
    else:
        resource_uuid = str(uuid.uuid4())
        _warn("Endpoint.id missing; generated new UUID")

    if not fhir.status:
        raise ValueError("Endpoint.status missing")
    if fhir.connectionType is None:
        raise ValueError("Endpoint.connectionType missing")
    if fhir.connectionType.code is None and fhir.connectionType.display is None:
        raise ValueError("Endpoint.connectionType missing code/display")

    conn = EndpointCoding(
        system=str(fhir.connectionType.system) if fhir.connectionType.system is not None else None,
        code=str(fhir.connectionType.code) if fhir.connectionType.code is not None else "(missing)",
        display=str(fhir.connectionType.display)
        if fhir.connectionType.display is not None
        else None,
    )

    # endpoint_rank extension
    endpoint_rank = None
    rank_exts = [e for e in list(fhir.extension or []) if str(e.url) == NDH_ENDPOINT_RANK_EXT_URL]
    if len(rank_exts) > 1:
        report.add("Endpoint.extension:endpoint-rank", len(rank_exts) - 1)
        _warn("Dropping extra endpoint-rank extensions beyond first")
        rank_exts = rank_exts[:1]
    if rank_exts:
        endpoint_rank = int(rank_exts[0].valueInteger) if rank_exts[0].valueInteger is not None else None

    # payload types
    payload_types: list[EndpointPayloadType] = []
    for cc in list(fhir.payloadType or []):
        if cc.coding and len(cc.coding) >= 1:
            c = cc.coding[0]
            code = str(c.code) if c.code is not None else None
            if code is None:
                report.add("Endpoint.payloadType", 1)
                _warn("Dropping payloadType entry without coding.code")
                continue
            if len(cc.coding) > 1:
                report.add("Endpoint.payloadType.coding", len(cc.coding) - 1)
            payload_types.append(
                EndpointPayloadType(
                    system=str(c.system) if c.system is not None else None,
                    code=code,
                    display=str(c.display) if c.display is not None else None,
                )
            )
        elif cc.text is not None:
            # treat text as code fallback
            payload_types.append(EndpointPayloadType(code=str(cc.text)))
        else:
            report.add("Endpoint.payloadType", 1)
            _warn("Dropping payloadType entry without coding/text")

    endpoint = Endpoint(
        resource_uuid=resource_uuid,
        status=str(fhir.status),
        connection_type=conn,
        name=str(fhir.name) if fhir.name is not None else None,
        endpoint_rank=endpoint_rank,
        payload_types=payload_types,
    )

    return endpoint, report


def endpoint_to_fhir_json(endpoint: Endpoint) -> dict[str, Any]:
    """Serialize canonical Endpoint back to FHIR JSON."""

    extension: list[Extension] = []
    if endpoint.endpoint_rank is not None:
        extension.append(
            Extension(url=NDH_ENDPOINT_RANK_EXT_URL, valueInteger=int(endpoint.endpoint_rank))
        )

    payload = [
        CodeableConcept(
            coding=[
                Coding(
                    system=p.system,
                    code=p.code,
                    display=p.display,
                )
            ]
        )
        for p in endpoint.payload_types
    ]

    fhir = EndpointResource(
        id=endpoint.resource_uuid,
        status=endpoint.status,
        connectionType=Coding(
            system=endpoint.connection_type.system,
            code=endpoint.connection_type.code,
            display=endpoint.connection_type.display,
        ),
        name=endpoint.name,
        payloadType=payload or None,
        extension=extension or None,
    )
    return fhir.model_dump(by_alias=True, exclude_none=True)
