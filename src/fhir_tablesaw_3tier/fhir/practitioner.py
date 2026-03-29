from __future__ import annotations

from typing import Any

import httpx

from fhir_tablesaw_3tier.domain.practitioner import (
    Address,
    ClinicianType,
    Credential,
    DroppedRepeatsReport,
    EndpointRef,
    LanguageProficiency,
    Practitioner,
    Telecom,
)
from fhir_tablesaw_3tier.fhir.constants import (
    NDH_COMM_PROFICIENCY_EXT_URL,
    NDH_ENDPOINT_REFERENCE_EXT_URL,
    US_CORE_ETHNICITY_URL,
    US_CORE_RACE_URL,
    US_NPI_SYSTEM,
)
from fhir_tablesaw_3tier.fhir.r4_models import PractitionerResource
from fhir_tablesaw_3tier.env import get_fhir_basic_auth


def _warn(msg: str) -> None:
    print(f"WARNING: {msg}")


def _extract_us_core_code(ext: dict[str, Any], url: str) -> str | None:
    """Extract a code from a US Core race/ethnicity extension.

    We do not have the CDC code system hooked up yet. For now, we accept that the
    code may be missing and fall back to placeholders as instructed.
    """

    if ext.get("url") != url:
        return None
    # Common US Core pattern: extension[].valueCoding.code
    for sub in ext.get("extension", []) or []:
        if sub.get("url") == "ombCategory" and "valueCoding" in sub:
            return sub["valueCoding"].get("code")
        if sub.get("url") == "detailed" and "valueCoding" in sub:
            return sub["valueCoding"].get("code")
        if sub.get("url") == "text" and "valueString" in sub:
            # fallback
            return sub.get("valueString")
    return None


def practitioner_from_fhir_json(
    raw: dict[str, Any], *, fhir_server_url: str | None = None
) -> tuple[Practitioner, DroppedRepeatsReport]:
    """Parse FHIR Practitioner JSON into canonical Practitioner.

    Notes:
    - Uses local minimal models built on top of **fhir-core**.
    - Handles selective flattening and warnings for dropped repeats.
    - Supports reference resolution via optional Basic Auth GET if a server URL is provided.
    """

    report = DroppedRepeatsReport()

    # Parse using fhir-core-based minimal model. (Includes `specialty` as an assumed IG change.)
    fhir = PractitionerResource.model_validate(raw)

    # resource_uuid (FHIR id)
    resource_uuid = str(fhir.id) if fhir.id is not None else None
    if not resource_uuid:
        _warn("Practitioner.id missing; using placeholder UUID all-zeros")
        resource_uuid = "00000000-0000-0000-0000-000000000000"

    # NPI
    npi: str | None = None
    identifiers = list(fhir.identifier or [])
    for ident in identifiers:
        if str(ident.system) == US_NPI_SYSTEM and ident.value:
            npi = str(ident.value)
            break
    if not npi:
        raise ValueError("Practitioner is missing required NPI identifier")

    # Active
    active_status = bool(fhir.active) if fhir.active is not None else None

    # Name flattening: legal + one alternate
    names = list(fhir.name or [])
    if len(names) > 2:
        dropped = names[2:]
        report.add("Practitioner.name", len(dropped))
        _warn(f"Dropping {len(dropped)} additional Practitioner.name entries: {dropped!r}")
        names = names[:2]

    def _flatten_name(n):
        given = list(n.given or [])
        first = str(given[0]) if len(given) >= 1 else None
        middle = str(given[1]) if len(given) >= 2 else None
        if len(given) > 2:
            report.add("HumanName.given", len(given) - 2)
            _warn(f"Dropping extra given names beyond first+middle: {given[2:]}")
        prefix = str((n.prefix or [None])[0]) if n.prefix else None
        if n.prefix and len(n.prefix) > 1:
            report.add("HumanName.prefix", len(n.prefix) - 1)
            _warn(f"Dropping extra prefixes beyond first: {n.prefix[1:]}")
        suffix = str((n.suffix or [None])[0]) if n.suffix else None
        if n.suffix and len(n.suffix) > 1:
            report.add("HumanName.suffix", len(n.suffix) - 1)
            _warn(f"Dropping extra suffixes beyond first: {n.suffix[1:]}")

        return {
            "first": first,
            "middle": middle,
            "last": str(n.family) if n.family else None,
            "prefix": prefix,
            "suffix": suffix,
        }

    legal = _flatten_name(names[0]) if len(names) >= 1 else {}
    other = _flatten_name(names[1]) if len(names) >= 2 else {}

    # Gender
    gender = str(fhir.gender) if fhir.gender is not None else None

    # Race/Ethnicity extensions. Placeholder values permitted.
    race_code = None
    ethnicity_code = None
    for ext in list(fhir.extension or []):
        ext_dict = ext.model_dump(by_alias=True, exclude_none=True)
        if race_code is None and ext_dict.get("url") == US_CORE_RACE_URL:
            race_code = _extract_us_core_code(ext_dict, US_CORE_RACE_URL)
        if ethnicity_code is None and ext_dict.get("url") == US_CORE_ETHNICITY_URL:
            ethnicity_code = _extract_us_core_code(ext_dict, US_CORE_ETHNICITY_URL)

    # Hardcoded placeholders per instructions
    if race_code is None:
        race_code = "purple"
    if ethnicity_code is None:
        ethnicity_code = "orange"

    # Telecom: keep only phone/fax
    telecoms: list[Telecom] = []
    for cp in list(fhir.telecom or []):
        if cp.system not in ("phone", "fax"):
            report.add("Practitioner.telecom", 1)
            _warn(f"Dropping unsupported telecom system {cp.system}: {cp.value}")
            continue
        if not cp.value:
            continue
        telecoms.append(Telecom(type=str(cp.system), value=str(cp.value)))

    # Addresses: take all, but only line[0]/line[1]
    addresses: list[Address] = []
    for addr in list(fhir.address or []):
        lines = list(addr.line or [])
        if len(lines) > 2:
            report.add("Address.line", len(lines) - 2)
            _warn(f"Dropping extra address lines beyond 2: {lines[2:]}")
            lines = lines[:2]
        addresses.append(
            Address(
                line1=str(lines[0]) if len(lines) >= 1 else None,
                line2=str(lines[1]) if len(lines) >= 2 else None,
                city=str(addr.city) if addr.city else None,
                state=str(addr.state) if addr.state else None,
                postal_code=str(addr.postalCode) if addr.postalCode else None,
                country=str(addr.country) if addr.country else None,
            )
        )

    # Endpoint references via NDH endpoint extension
    endpoint_refs: list[EndpointRef] = []
    for ext in list(fhir.extension or []):
        ext_dict = ext.model_dump(by_alias=True, exclude_none=True)
        if ext_dict.get("url") != NDH_ENDPOINT_REFERENCE_EXT_URL:
            continue
        ref = ext_dict.get("valueReference") or {}
        reference = ref.get("reference")  # e.g. "Endpoint/<id>"
        if not reference:
            continue
        endpoint_id = str(reference).split("/")[-1]
        endpoint_refs.append(EndpointRef(resource_uuid=endpoint_id))

    # Resolve endpoints if server URL provided (store just uuid for now)
    if fhir_server_url:
        # try to resolve and validate existence
        with httpx.Client(
            base_url=fhir_server_url,
            timeout=10.0,
            auth=get_fhir_basic_auth(),
            headers={"Accept": "application/fhir+json"},
        ) as client:
            for e in endpoint_refs:
                try:
                    r = client.get(f"Endpoint/{e.resource_uuid}")
                    if r.status_code >= 400:
                        _warn(
                            f"Failed to resolve Endpoint/{e.resource_uuid} from server: {r.status_code}"
                        )
                except Exception as ex:  # noqa: BLE001
                    _warn(f"Error resolving Endpoint/{e.resource_uuid}: {ex}")

    # Language proficiencies (communication + proficiency extension)
    language_proficiencies: list[LanguageProficiency] = []
    for comm in list(fhir.communication or []):
        lang_cc = comm.language
        lang_code = None
        if lang_cc and lang_cc.coding:
            lang_code = str(lang_cc.coding[0].code) if lang_cc.coding[0].code else None
        if lang_code is None and lang_cc and lang_cc.text:
            lang_code = str(lang_cc.text)
        if lang_code is None:
            continue

        proficiency = None
        for ext in list(comm.extension or []):
            ext_dict = ext.model_dump(by_alias=True, exclude_none=True)
            if ext_dict.get("url") == NDH_COMM_PROFICIENCY_EXT_URL:
                # treat as valueCode or valueString for now
                proficiency = ext_dict.get("valueCode") or ext_dict.get("valueString")

        language_proficiencies.append(
            LanguageProficiency(language_code=lang_code, proficiency_level=proficiency)
        )

    # Clinician types from specialty list (pretend field)
    clinician_types: list[ClinicianType] = []
    specialty_list = list(fhir.specialty or [])
    for spec in specialty_list:
        if spec.coding and spec.coding[0].code:
            clinician_types.append(ClinicianType(code=str(spec.coding[0].code)))
        elif spec.text:
            clinician_types.append(ClinicianType(code=str(spec.text)))
        else:
            report.add("Practitioner.specialty", 1)
            _warn(f"Dropping specialty entry without code/text: {spec.model_dump()}" )

    # Credentials from qualification.code (array)
    credentials: list[Credential] = []
    for qual in list(fhir.qualification or []):
        code = qual.code
        if not code:
            continue
        # take code.text first, then coding[0].code
        value = None
        if code.text:
            value = str(code.text)
        elif code.coding:
            value = str(code.coding[0].code) if code.coding[0].code else None
        if value:
            credentials.append(Credential(value=value))

    # CMS booleans: placeholder extension url base; parse any boolean extension values
    cms_flags = {
        "is_cms_enrolled": None,
        "is_cms_ial2_verified": None,
        "is_participating_in_cms_aligned_data_networks": None,
    }
    for ext in list(fhir.extension or []):
        ext_dict = ext.model_dump(by_alias=True, exclude_none=True)
        url = ext_dict.get("url")
        if url != "https://example.com/extension_url/":
            continue
        # No further discriminator provided, so we can’t map reliably yet.
        # Leave as None.
        # (Once URLs are distinct per flag, implement mapping.)
        pass

    practitioner = Practitioner(
        resource_uuid=resource_uuid,
        npi=npi,
        active_status=active_status,
        first_name=legal.get("first"),
        middle_name=legal.get("middle"),
        last_name=legal.get("last"),
        prefix=legal.get("prefix"),
        non_clinical_suffix=legal.get("suffix"),
        other_first_name=other.get("first"),
        other_middle_name=other.get("middle"),
        other_last_name=other.get("last"),
        other_prefix=other.get("prefix"),
        other_non_clinical_suffix=other.get("suffix"),
        gender=gender,
        race_code=race_code,
        ethnicity_code=ethnicity_code,
        is_cms_enrolled=cms_flags["is_cms_enrolled"],
        is_cms_ial2_verified=cms_flags["is_cms_ial2_verified"],
        is_participating_in_cms_aligned_data_networks=cms_flags[
            "is_participating_in_cms_aligned_data_networks"
        ],
        endpoints=endpoint_refs,
        addresses=addresses,
        telecoms=telecoms,
        clinician_types=clinician_types,
        credentials=credentials,
        language_proficiencies=language_proficiencies,
    )

    return practitioner, report


def practitioner_to_fhir_json(practitioner: Practitioner) -> dict[str, Any]:
    """Serialize canonical Practitioner to FHIR JSON (dict).

    This uses our minimal fhir-core models to produce a clean JSON dict.

    NOTE: The CMS boolean extensions are not emitted yet because the instruction
    currently provides only a shared placeholder URL (no per-flag discriminator).
    """

    from fhir_tablesaw_3tier.fhir.constants import US_NPI_SYSTEM  # avoid cycle
    from fhir_tablesaw_3tier.fhir.r4_models import (
        CodeableConcept,
        Coding,
        Identifier,
        HumanName,
        ContactPoint,
        Address as FhirAddress,
        PractitionerQualification,
        PractitionerCommunication,
        Extension,
        Reference,
    )

    identifier = [Identifier(system=US_NPI_SYSTEM, value=practitioner.npi)]

    names: list[HumanName] = []
    if any(
        [
            practitioner.first_name,
            practitioner.middle_name,
            practitioner.last_name,
            practitioner.prefix,
            practitioner.non_clinical_suffix,
        ]
    ):
        names.append(
            HumanName(
                family=practitioner.last_name,
                given=[x for x in [practitioner.first_name, practitioner.middle_name] if x],
                prefix=[practitioner.prefix] if practitioner.prefix else None,
                suffix=[practitioner.non_clinical_suffix]
                if practitioner.non_clinical_suffix
                else None,
            )
        )
    if any(
        [
            practitioner.other_first_name,
            practitioner.other_middle_name,
            practitioner.other_last_name,
            practitioner.other_prefix,
            practitioner.other_non_clinical_suffix,
        ]
    ):
        names.append(
            HumanName(
                family=practitioner.other_last_name,
                given=[
                    x
                    for x in [practitioner.other_first_name, practitioner.other_middle_name]
                    if x
                ],
                prefix=[practitioner.other_prefix] if practitioner.other_prefix else None,
                suffix=[practitioner.other_non_clinical_suffix]
                if practitioner.other_non_clinical_suffix
                else None,
            )
        )

    telecom = [ContactPoint(system=t.type, value=t.value) for t in practitioner.telecoms]

    address = [
        FhirAddress(
            line=[x for x in [a.line1, a.line2] if x] or None,
            city=a.city,
            state=a.state,
            postalCode=a.postal_code,
            country=a.country,
        )
        for a in practitioner.addresses
    ]

    qualification = [
        PractitionerQualification(code=CodeableConcept(text=c.value)) for c in practitioner.credentials
    ]

    specialty = [
        CodeableConcept(coding=[Coding(code=ct.code)]) for ct in practitioner.clinician_types
    ]

    communication: list[PractitionerCommunication] = []
    for lp in practitioner.language_proficiencies:
        ext = None
        if lp.proficiency_level is not None:
            ext = [Extension(url=NDH_COMM_PROFICIENCY_EXT_URL, valueCode=lp.proficiency_level)]
        communication.append(
            PractitionerCommunication(
                extension=ext,
                language=CodeableConcept(coding=[Coding(code=lp.language_code)]),
            )
        )

    extension: list[Extension] = []
    for e in practitioner.endpoints:
        extension.append(
            Extension(
                url=NDH_ENDPOINT_REFERENCE_EXT_URL,
                valueReference=Reference(reference=f"Endpoint/{e.resource_uuid}"),
            )
        )

    # Race/Ethnicity are real extension URLs, but code values are placeholders.
    # Emit the simplest US Core-compatible shape with ombCategory.
    if practitioner.race_code is not None:
        extension.append(
            Extension(
                url=US_CORE_RACE_URL,
                extension=[
                    Extension(
                        url="ombCategory",
                        valueCoding=Coding(code=practitioner.race_code),
                    )
                ],
            )
        )
    if practitioner.ethnicity_code is not None:
        extension.append(
            Extension(
                url=US_CORE_ETHNICITY_URL,
                extension=[
                    Extension(
                        url="ombCategory",
                        valueCoding=Coding(code=practitioner.ethnicity_code),
                    )
                ],
            )
        )

    fhir = PractitionerResource(
        id=practitioner.resource_uuid,
        identifier=identifier,
        active=practitioner.active_status,
        name=names or None,
        telecom=telecom or None,
        address=address or None,
        gender=practitioner.gender,
        communication=communication or None,
        qualification=qualification or None,
        specialty=specialty or None,
        extension=extension or None,
    )

    return fhir.model_dump(by_alias=True, exclude_none=True)
