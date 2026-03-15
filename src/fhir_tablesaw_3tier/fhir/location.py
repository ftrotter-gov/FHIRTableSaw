from __future__ import annotations

import base64
from typing import Any

import uuid

from fhir_tablesaw_3tier.domain.dropped_repeats import DroppedRepeatsReport
from fhir_tablesaw_3tier.domain.location import (
    Location,
    LocationAccessibility,
    LocationBoundaryGeoJson,
    LocationHoursOfOperation,
    LocationPosition,
    LocationTelecom,
)
from fhir_tablesaw_3tier.fhir.constants import (
    LOCATION_BOUNDARY_GEOJSON_EXT_URL,
    NDH_LOCATION_ACCESSIBILITY_EXT_URL,
    NDH_LOCATION_NEWPATIENTS_EXT_URL,
    NDH_LOCATION_VERIFICATION_STATUS_EXT_URL,
)
from fhir_tablesaw_3tier.fhir.r4_models import (
    Address,
    Attachment,
    CodeableConcept,
    Coding,
    ContactPoint,
    Extension,
    LocationHoursOfOperation as FhirHoursOfOperation,
    LocationPosition as FhirPosition,
    LocationResource,
    Reference,
)


def _warn(msg: str) -> None:
    print(f"WARNING: {msg}")


def _extract_uuid(ref: Reference | None) -> str | None:
    if ref is None or ref.reference is None:
        return None
    return str(ref.reference).split("/")[-1]


def _parse_boundary_geojson(ext: Extension) -> LocationBoundaryGeoJson | None:
    """Parse core FHIR location-boundary-geojson extension.

    SD says valueAttachment.
    We'll accept:
    - valueAttachment.data (base64) -> decode to UTF-8
    - valueAttachment.url -> store as "url:<...>"
    """

    att = ext.valueAttachment
    if att is None:
        return None

    if att.data is not None:
        try:
            raw = base64.b64decode(str(att.data))
            txt = raw.decode("utf-8", errors="replace")
            return LocationBoundaryGeoJson(geojson_text=txt)
        except Exception as ex:  # noqa: BLE001
            _warn(f"Failed to decode boundary geojson attachment.data: {ex}")
            return None
    if att.url is not None:
        return LocationBoundaryGeoJson(geojson_text=f"url:{att.url}")
    return None


def location_from_fhir_json(
    raw: dict[str, Any], *, fhir_server_url: str | None = None
) -> tuple[Location, DroppedRepeatsReport]:
    """Parse FHIR Location JSON into canonical Location."""

    _ = fhir_server_url
    report = DroppedRepeatsReport()
    fhir = LocationResource.model_validate(raw)

    # id
    if fhir.id:
        resource_uuid = str(fhir.id)
    else:
        resource_uuid = str(uuid.uuid4())
        _warn("Location.id missing; generated new UUID")

    if not fhir.status:
        raise ValueError("Location.status missing")
    if not fhir.name:
        raise ValueError("Location.name missing")

    managing_org_uuid = _extract_uuid(fhir.managingOrganization)
    part_of_uuid = _extract_uuid(fhir.partOf)

    # address 0..1
    addr_line1 = addr_line2 = addr_city = addr_state = addr_postal = addr_country = None
    if fhir.address is not None:
        lines = list(fhir.address.line or [])
        if len(lines) > 2:
            report.add("Location.address.line", len(lines) - 2)
            _warn("Dropping extra Location.address.line entries beyond first two")
            lines = lines[:2]
        addr_line1 = str(lines[0]) if len(lines) >= 1 else None
        addr_line2 = str(lines[1]) if len(lines) >= 2 else None
        addr_city = str(fhir.address.city) if fhir.address.city is not None else None
        addr_state = str(fhir.address.state) if fhir.address.state is not None else None
        addr_postal = (
            str(fhir.address.postalCode) if fhir.address.postalCode is not None else None
        )
        addr_country = str(fhir.address.country) if fhir.address.country is not None else None
    else:
        _warn("Location.address missing")

    # position 0..1 but lat/long 1..1 if present
    position = None
    if fhir.position is not None:
        if fhir.position.latitude is None or fhir.position.longitude is None:
            _warn("Location.position present but missing latitude/longitude; dropping position")
        else:
            try:
                position = LocationPosition(
                    latitude=float(str(fhir.position.latitude)),
                    longitude=float(str(fhir.position.longitude)),
                    altitude=float(str(fhir.position.altitude))
                    if fhir.position.altitude is not None
                    else None,
                )
            except ValueError:
                _warn("Invalid Location.position numeric values; dropping position")

    # telecom: keep phone/fax only
    telecoms: list[LocationTelecom] = []
    for t in list(fhir.telecom or []):
        if t.system not in ("phone", "fax"):
            report.add("Location.telecom", 1)
            _warn(f"Dropping unsupported Location.telecom system {t.system}: {t.value}")
            continue
        if not t.value:
            continue
        telecoms.append(LocationTelecom(type=str(t.system), value=str(t.value)))

    # endpoints
    endpoints: list[str] = []
    for r in list(fhir.endpoint or []):
        if r.reference:
            endpoints.append(str(r.reference).split("/")[-1])

    # hoursOfOperation
    hours: list[LocationHoursOfOperation] = []
    for h in list(fhir.hoursOfOperation or []):
        # daysOfWeek is ignored in canonical for now (we don't have a column)
        hours.append(
            LocationHoursOfOperation(
                all_day=bool(h.allDay) if h.allDay is not None else None,
                opening_time=str(h.openingTime) if h.openingTime is not None else None,
                closing_time=str(h.closingTime) if h.closingTime is not None else None,
            )
        )

    # extensions
    boundary_geojson: LocationBoundaryGeoJson | None = None
    accessibility: list[LocationAccessibility] = []
    newpatients: list[dict[str, Any]] = []
    verification_status: dict[str, Any] | None = None

    for ext in list(fhir.extension or []):
        url = str(ext.url)
        if url == LOCATION_BOUNDARY_GEOJSON_EXT_URL:
            boundary_geojson = _parse_boundary_geojson(ext)
        elif url == NDH_LOCATION_ACCESSIBILITY_EXT_URL:
            cc = ext.valueCodeableConcept
            if cc is None:
                report.add("Location.extension:accessibility", 1)
                _warn("Accessibility extension missing valueCodeableConcept; dropping")
                continue
            if cc.coding and len(cc.coding) >= 1:
                c = cc.coding[0]
                code = str(c.code) if c.code is not None else None
                if code is None:
                    report.add("Location.extension:accessibility", 1)
                    _warn("Accessibility coding missing code; dropping")
                    continue
                accessibility.append(
                    LocationAccessibility(
                        system=str(c.system) if c.system is not None else None,
                        code=code,
                        display=str(c.display) if c.display is not None else None,
                    )
                )
            elif cc.text is not None:
                accessibility.append(LocationAccessibility(code=str(cc.text)))
            else:
                report.add("Location.extension:accessibility", 1)
                _warn("Accessibility CodeableConcept missing coding/text; dropping")
        elif url == NDH_LOCATION_NEWPATIENTS_EXT_URL:
            # complex extension: keep raw
            newpatients.append(ext.model_dump(by_alias=True, exclude_none=True))
        elif url == NDH_LOCATION_VERIFICATION_STATUS_EXT_URL:
            verification_status = ext.model_dump(by_alias=True, exclude_none=True)

    if boundary_geojson is None:
        _warn("Location boundary GeoJSON extension missing")

    canonical = Location(
        resource_uuid=resource_uuid,
        status=str(fhir.status),
        name=str(fhir.name),
        description=str(fhir.description) if fhir.description is not None else None,
        availability_exceptions=str(fhir.availabilityExceptions)
        if fhir.availabilityExceptions is not None
        else None,
        managing_organization_resource_uuid=managing_org_uuid,
        part_of_location_resource_uuid=part_of_uuid,
        address_line1=addr_line1,
        address_line2=addr_line2,
        address_city=addr_city,
        address_state=addr_state,
        address_postal_code=addr_postal,
        address_country=addr_country,
        position=position,
        boundary_geojson=boundary_geojson,
        accessibility=accessibility,
        telecoms=telecoms,
        endpoints=endpoints,
        hours_of_operation=hours,
        newpatients=newpatients,
        verification_status=verification_status,
    )

    return canonical, report


def location_to_fhir_json(location: Location) -> dict[str, Any]:
    """Serialize canonical Location to FHIR JSON."""

    address = None
    if any(
        [
            location.address_line1,
            location.address_line2,
            location.address_city,
            location.address_state,
            location.address_postal_code,
            location.address_country,
        ]
    ):
        lines = [x for x in [location.address_line1, location.address_line2] if x]
        address = Address(
            line=lines or None,
            city=location.address_city,
            state=location.address_state,
            postalCode=location.address_postal_code,
            country=location.address_country,
        )

    telecom = [ContactPoint(system=t.type, value=t.value) for t in location.telecoms]
    endpoint = [Reference(reference=f"Endpoint/{u}") for u in location.endpoints]

    managing_org = (
        Reference(reference=f"Organization/{location.managing_organization_resource_uuid}")
        if location.managing_organization_resource_uuid
        else None
    )
    part_of = (
        Reference(reference=f"Location/{location.part_of_location_resource_uuid}")
        if location.part_of_location_resource_uuid
        else None
    )

    position = None
    if location.position is not None:
        position = FhirPosition(
            latitude=str(location.position.latitude),
            longitude=str(location.position.longitude),
            altitude=str(location.position.altitude) if location.position.altitude is not None else None,
        )

    hours = [
        FhirHoursOfOperation(
            allDay=h.all_day,
            openingTime=h.opening_time,
            closingTime=h.closing_time,
        )
        for h in location.hours_of_operation
    ]

    extension: list[Extension] = []

    # boundary geojson
    if location.boundary_geojson is not None:
        # encode as attachment.data (base64) for round-tripping
        b64 = base64.b64encode(location.boundary_geojson.geojson_text.encode("utf-8")).decode(
            "ascii"
        )
        extension.append(
            Extension(
                url=LOCATION_BOUNDARY_GEOJSON_EXT_URL,
                valueAttachment=Attachment(contentType="application/geo+json", data=b64),
            )
        )

    # accessibility
    for a in location.accessibility:
        cc = CodeableConcept(
            coding=[Coding(system=a.system, code=a.code, display=a.display)]
            if a.code
            else None
        )
        extension.append(Extension(url=NDH_LOCATION_ACCESSIBILITY_EXT_URL, valueCodeableConcept=cc))

    # overflow extensions we preserved
    for raw_ext in location.newpatients:
        try:
            extension.append(Extension.model_validate(raw_ext))
        except Exception:  # noqa: BLE001
            _warn("Unable to re-validate newpatients extension; dropping")
    if location.verification_status:
        try:
            extension.append(Extension.model_validate(location.verification_status))
        except Exception:  # noqa: BLE001
            _warn("Unable to re-validate verification-status extension; dropping")

    fhir = LocationResource(
        id=location.resource_uuid,
        status=location.status,
        name=location.name,
        description=location.description,
        address=address,
        telecom=telecom or None,
        managingOrganization=managing_org,
        partOf=part_of,
        position=position,
        hoursOfOperation=hours or None,
        availabilityExceptions=location.availability_exceptions,
        endpoint=endpoint or None,
        extension=extension or None,
    )
    return fhir.model_dump(by_alias=True, exclude_none=True)
