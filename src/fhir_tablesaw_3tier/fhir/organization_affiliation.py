from __future__ import annotations

from typing import Any

import uuid

from fhir_tablesaw_3tier.domain.organization_affiliation import (
    AffiliationCode,
    AffiliationTelecom,
    DroppedRepeatsReport,
    OrganizationAffiliation,
    Specialty,
)
from fhir_tablesaw_3tier.fhir.r4_models import (
    CodeableConcept,
    Coding,
    ContactPoint,
    OrganizationAffiliationResource,
    Reference,
)


def _warn(msg: str) -> None:
    print(f"WARNING: {msg}")


def _extract_uuid(ref: Reference | None) -> str | None:
    if ref is None or ref.reference is None:
        return None
    return str(ref.reference).split("/")[-1]


def organization_affiliation_from_fhir_json(
    raw: dict[str, Any], *, fhir_server_url: str | None = None
) -> tuple[OrganizationAffiliation, DroppedRepeatsReport]:
    """Parse FHIR OrganizationAffiliation JSON into canonical model.

    - Requires both organization and participatingOrganization in CMS practice.
    - Restricts telecom to phone/fax, drops others with report.
    - Uses exactly one code CodeableConcept, drops extras with report.
    """

    _ = fhir_server_url  # reserved for future reference resolution

    report = DroppedRepeatsReport()
    fhir = OrganizationAffiliationResource.model_validate(raw)

    # resource_uuid (FHIR id)
    if fhir.id:
        resource_uuid = str(fhir.id)
    else:
        # per clarified rule: if missing, generate a new UUID
        resource_uuid = str(uuid.uuid4())
        _warn("OrganizationAffiliation.id missing; generated new UUID")

    primary_uuid = _extract_uuid(fhir.organization)
    participating_uuid = _extract_uuid(fhir.participatingOrganization)

    if not primary_uuid or not participating_uuid:
        raise ValueError(
            "OrganizationAffiliation requires both organization and participatingOrganization"
        )

    # code: exactly one CodeableConcept
    codes = list(fhir.code or [])
    if not codes:
        raise ValueError("OrganizationAffiliation.code missing (required in CMS practice)")
    if len(codes) > 1:
        report.add("OrganizationAffiliation.code", len(codes) - 1)
        _warn(f"Dropping extra OrganizationAffiliation.code entries beyond first")
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
        raise ValueError("OrganizationAffiliation.code must provide coding.code or text")

    aff_code = AffiliationCode(system=system, code=code, display=display)

    # specialties: repeated CodeableConcept -> NUCC taxonomy codes as strings
    specialties: list[Specialty] = []
    for s in list(fhir.specialty or []):
        s_code = None
        if s.coding and s.coding[0].code is not None:
            s_code = str(s.coding[0].code)
        elif s.text is not None:
            s_code = str(s.text)
        if s_code is None:
            report.add("OrganizationAffiliation.specialty", 1)
            _warn("Dropping specialty entry without code/text")
            continue
        specialties.append(Specialty(code=s_code))

    # telecom phone/fax only
    telecoms: list[AffiliationTelecom] = []
    for t in list(fhir.telecom or []):
        if t.system not in ("phone", "fax"):
            report.add("OrganizationAffiliation.telecom", 1)
            _warn(f"Dropping unsupported telecom system {t.system}: {t.value}")
            continue
        if not t.value:
            continue
        telecoms.append(AffiliationTelecom(type=str(t.system), value=str(t.value)))

    # endpoints
    endpoints: list[dict] = []
    for r in list(getattr(fhir, "endpoint", None) or []):
        if r.reference:
            endpoints.append({"resource_uuid": str(r.reference).split("/")[-1]})

    canonical = OrganizationAffiliation(
        resource_uuid=resource_uuid,
        active=bool(fhir.active) if fhir.active is not None else None,
        primary_organization_resource_uuid=primary_uuid,
        participating_organization_resource_uuid=participating_uuid,
        code=aff_code,
        specialties=specialties,
        telecoms=telecoms,
        endpoints=endpoints,
    )

    return canonical, report


def organization_affiliation_to_fhir_json(aff: OrganizationAffiliation) -> dict[str, Any]:
    """Serialize canonical OrganizationAffiliation to FHIR JSON."""

    code_cc = CodeableConcept(
        coding=[
            Coding(system=aff.code.system, code=aff.code.code, display=aff.code.display)
        ]
    )

    specialty_cc = [CodeableConcept(coding=[Coding(code=s.code)]) for s in aff.specialties]
    telecom = [ContactPoint(system=t.type, value=t.value) for t in aff.telecoms]

    endpoint = [
        Reference(reference=f"Endpoint/{e['resource_uuid']}")
        for e in (getattr(aff, "endpoints", []) or [])
        if isinstance(e, dict) and e.get("resource_uuid")
    ]

    fhir = OrganizationAffiliationResource(
        id=aff.resource_uuid,
        active=aff.active,
        organization=Reference(reference=f"Organization/{aff.primary_organization_resource_uuid}"),
        participatingOrganization=Reference(
            reference=f"Organization/{aff.participating_organization_resource_uuid}"
        ),
        code=[code_cc],
        specialty=specialty_cc or None,
        telecom=telecom or None,
        endpoint=endpoint or None,
    )

    return fhir.model_dump(by_alias=True, exclude_none=True)
