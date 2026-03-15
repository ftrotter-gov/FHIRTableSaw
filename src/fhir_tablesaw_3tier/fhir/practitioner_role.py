from __future__ import annotations

from typing import Any

import uuid

from fhir_tablesaw_3tier.domain.practitioner_role import (
    DroppedRepeatsReport,
    EndpointRef,
    PractitionerRole,
    RoleCode,
    RoleTelecom,
    Specialty,
)
from fhir_tablesaw_3tier.fhir.constants import (
    PRACTITIONER_ROLE_ACCEPTING_NEW_PATIENTS_EXT_URL,
    PRACTITIONER_ROLE_CMS_IAL2_VALIDATED_EXT_URL,
    PRACTITIONER_ROLE_CMS_PECOS_VALIDATED_EXT_URL,
    PRACTITIONER_ROLE_HAS_CMS_ALIGNED_DATA_NETWORK_EXT_URL,
    PRACTITIONER_ROLE_RATING_EXT_URL,
)
from fhir_tablesaw_3tier.fhir.r4_models import (
    CodeableConcept,
    Coding,
    ContactPoint,
    Extension,
    PractitionerRoleResource,
    Reference,
)


def _warn(msg: str) -> None:
    print(f"WARNING: {msg}")


def _extract_uuid(ref: Reference | None) -> str | None:
    if ref is None or ref.reference is None:
        return None
    return str(ref.reference).split("/")[-1]


def practitioner_role_from_fhir_json(
    raw: dict[str, Any], *, fhir_server_url: str | None = None
) -> tuple[PractitionerRole, DroppedRepeatsReport]:
    """Parse FHIR PractitionerRole JSON into canonical model."""

    _ = fhir_server_url
    report = DroppedRepeatsReport()
    fhir = PractitionerRoleResource.model_validate(raw)

    if fhir.id:
        resource_uuid = str(fhir.id)
    else:
        resource_uuid = str(uuid.uuid4())
        _warn("PractitionerRole.id missing; generated new UUID")

    pract_uuid = _extract_uuid(fhir.practitioner)
    org_uuid = _extract_uuid(fhir.organization)
    if not pract_uuid or not org_uuid:
        raise ValueError("PractitionerRole requires practitioner and organization references")

    # active is required in NDH; allow None but warn
    if fhir.active is None:
        _warn("PractitionerRole.active missing")

    # code: exactly one
    codes = list(fhir.code or [])
    if not codes:
        raise ValueError("PractitionerRole.code missing")
    if len(codes) > 1:
        report.add("PractitionerRole.code", len(codes) - 1)
        _warn("Dropping extra PractitionerRole.code entries beyond first")
        codes = codes[:1]

    code_cc = codes[0]
    system = None
    code = None
    display = None
    if code_cc.coding and len(code_cc.coding) >= 1:
        c = code_cc.coding[0]
        system = str(c.system) if c.system is not None else None
        code = str(c.code) if c.code is not None else None
        display = str(c.display) if c.display is not None else None
    if code is None and code_cc.text is not None:
        code = str(code_cc.text)
    if code is None:
        raise ValueError("PractitionerRole.code must provide coding.code or text")

    role_code = RoleCode(system=system, code=code, display=display)

    # specialty: many NUCC codes
    specialties: list[Specialty] = []
    for s in list(fhir.specialty or []):
        s_code = None
        if s.coding and s.coding[0].code is not None:
            s_code = str(s.coding[0].code)
        elif s.text is not None:
            s_code = str(s.text)
        if s_code is None:
            report.add("PractitionerRole.specialty", 1)
            _warn("Dropping specialty entry without code/text")
            continue
        specialties.append(Specialty(code=s_code))

    # telecom phone/fax only
    telecoms: list[RoleTelecom] = []
    for t in list(fhir.telecom or []):
        if t.system not in ("phone", "fax"):
            report.add("PractitionerRole.telecom", 1)
            _warn(f"Dropping unsupported telecom system {t.system}: {t.value}")
            continue
        if not t.value:
            continue
        telecoms.append(RoleTelecom(type=str(t.system), value=str(t.value)))

    # endpoints: many
    endpoints: list[EndpointRef] = []
    for r in list(fhir.endpoint or []):
        if r.reference:
            endpoints.append(EndpointRef(resource_uuid=str(r.reference).split("/")[-1]))

    # location: CMS constrain to 0..1
    location_uuid = None
    locs = list(fhir.location or [])
    if len(locs) > 1:
        report.add("PractitionerRole.location", len(locs) - 1)
        _warn("Dropping extra PractitionerRole.location entries beyond first")
        locs = locs[:1]
    if locs:
        location_uuid = _extract_uuid(locs[0])

    # healthcareService: CMS constrain to 0..1
    hs_uuid = None
    hss = list(fhir.healthcareService or [])
    if len(hss) > 1:
        report.add("PractitionerRole.healthcareService", len(hss) - 1)
        _warn("Dropping extra PractitionerRole.healthcareService entries beyond first")
        hss = hss[:1]
    if hss:
        hs_uuid = _extract_uuid(hss[0])

    # extensions
    accepting_new_patients = None
    rating = None
    cms_pecos_validated = None
    cms_ial2_validated = None
    has_cms_aligned_data_network = None

    for ext in list(fhir.extension or []):
        url = str(ext.url)
        if url == PRACTITIONER_ROLE_ACCEPTING_NEW_PATIENTS_EXT_URL:
            accepting_new_patients = (
                bool(ext.valueBoolean) if ext.valueBoolean is not None else None
            )
        elif url == PRACTITIONER_ROLE_RATING_EXT_URL:
            if ext.valueInteger is not None:
                rating = int(ext.valueInteger)
            elif ext.valueString is not None:
                try:
                    rating = int(str(ext.valueString))
                except ValueError:
                    _warn(f"Invalid PractitionerRole rating valueString: {ext.valueString}")
        elif url == PRACTITIONER_ROLE_CMS_PECOS_VALIDATED_EXT_URL:
            cms_pecos_validated = bool(ext.valueBoolean) if ext.valueBoolean is not None else None
        elif url == PRACTITIONER_ROLE_CMS_IAL2_VALIDATED_EXT_URL:
            cms_ial2_validated = bool(ext.valueBoolean) if ext.valueBoolean is not None else None
        elif url == PRACTITIONER_ROLE_HAS_CMS_ALIGNED_DATA_NETWORK_EXT_URL:
            has_cms_aligned_data_network = (
                bool(ext.valueBoolean) if ext.valueBoolean is not None else None
            )

    canonical = PractitionerRole(
        resource_uuid=resource_uuid,
        active=bool(fhir.active) if fhir.active is not None else None,
        practitioner_resource_uuid=pract_uuid,
        organization_resource_uuid=org_uuid,
        code=role_code,
        specialties=specialties,
        telecoms=telecoms,
        endpoints=endpoints,
        accepting_new_patients=accepting_new_patients,
        rating=rating,
        cms_pecos_validated=cms_pecos_validated,
        cms_ial2_validated=cms_ial2_validated,
        has_cms_aligned_data_network=has_cms_aligned_data_network,
        location_resource_uuid=location_uuid,
        healthcare_service_resource_uuid=hs_uuid,
    )
    return canonical, report


def practitioner_role_to_fhir_json(role: PractitionerRole) -> dict[str, Any]:
    """Serialize canonical PractitionerRole to FHIR JSON."""

    code_cc = CodeableConcept(
        coding=[
            Coding(system=role.code.system, code=role.code.code, display=role.code.display)
        ]
    )
    specialty_cc = [CodeableConcept(coding=[Coding(code=s.code)]) for s in role.specialties]
    telecom = [ContactPoint(system=t.type, value=t.value) for t in role.telecoms]
    endpoint = [Reference(reference=f"Endpoint/{e.resource_uuid}") for e in role.endpoints]

    location = None
    if role.location_resource_uuid:
        location = [Reference(reference=f"Location/{role.location_resource_uuid}")]

    healthcare_service = None
    if role.healthcare_service_resource_uuid:
        healthcare_service = [
            Reference(reference=f"HealthcareService/{role.healthcare_service_resource_uuid}")
        ]

    extension: list[Extension] = []
    if role.accepting_new_patients is not None:
        extension.append(
            Extension(
                url=PRACTITIONER_ROLE_ACCEPTING_NEW_PATIENTS_EXT_URL,
                valueBoolean=role.accepting_new_patients,
            )
        )
    if role.rating is not None:
        extension.append(Extension(url=PRACTITIONER_ROLE_RATING_EXT_URL, valueInteger=role.rating))

    if role.cms_pecos_validated is not None:
        extension.append(
            Extension(
                url=PRACTITIONER_ROLE_CMS_PECOS_VALIDATED_EXT_URL,
                valueBoolean=role.cms_pecos_validated,
            )
        )
    if role.cms_ial2_validated is not None:
        extension.append(
            Extension(
                url=PRACTITIONER_ROLE_CMS_IAL2_VALIDATED_EXT_URL,
                valueBoolean=role.cms_ial2_validated,
            )
        )
    if role.has_cms_aligned_data_network is not None:
        extension.append(
            Extension(
                url=PRACTITIONER_ROLE_HAS_CMS_ALIGNED_DATA_NETWORK_EXT_URL,
                valueBoolean=role.has_cms_aligned_data_network,
            )
        )

    fhir = PractitionerRoleResource(
        id=role.resource_uuid,
        active=role.active,
        practitioner=Reference(reference=f"Practitioner/{role.practitioner_resource_uuid}"),
        organization=Reference(reference=f"Organization/{role.organization_resource_uuid}"),
        code=[code_cc],
        specialty=specialty_cc or None,
        telecom=telecom or None,
        endpoint=endpoint or None,
        location=location,
        healthcareService=healthcare_service,
        extension=extension or None,
    )

    return fhir.model_dump(by_alias=True, exclude_none=True)
