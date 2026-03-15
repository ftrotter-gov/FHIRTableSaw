from __future__ import annotations

from typing import Any

from fhir_tablesaw_3tier.domain.organization_clinical import (
    ClinicalOrganization,
    DroppedRepeatsReport,
    EndpointRef,
    OrgAlias,
    OrgContact,
    OrgTelecom,
)
from fhir_tablesaw_3tier.domain.practitioner import Address as CanonicalAddress
from fhir_tablesaw_3tier.fhir.constants import (
    HL7_ORG_TYPE_PROV_CODE,
    HL7_ORG_TYPE_SYSTEM,
    NDH_LOGO_EXT_URL,
    NDH_ORGANIZATION_PROFILE_URL,
    ORG_ALIAS_TYPE_EXT_URL,
    ORG_CMS_IAL2_VALIDATED_EXT_URL,
    ORG_CMS_PECOS_VALIDATED_EXT_URL,
    ORG_HAS_CMS_ALIGNED_DATA_NETWORK_EXT_URL,
    ORG_RATING_EXT_URL,
    US_NPI_SYSTEM,
)
from fhir_tablesaw_3tier.fhir.r4_models import (
    Address,
    Coding,
    ContactPoint,
    Element,
    Extension,
    HumanName,
    Identifier,
    Meta,
    OrganizationContact,
    OrganizationResource,
    OrganizationType,
    Reference,
)


def _warn(msg: str) -> None:
    print(f"WARNING: {msg}")


def clinical_organization_from_fhir_json(
    raw: dict[str, Any], *, fhir_server_url: str | None = None
) -> tuple[ClinicalOrganization, DroppedRepeatsReport]:
    """Parse FHIR Organization JSON into canonical ClinicalOrganization.

    Notes:
    - Uses minimal fhir-core based models.
    - Ignores unsupported telecom types (email, url, etc.) and reports drops.
    """

    report = DroppedRepeatsReport()
    org = OrganizationResource.model_validate(raw)

    if not org.id:
        _warn("Organization.id missing; using placeholder UUID all-zeros")
        resource_uuid = "00000000-0000-0000-0000-000000000000"
    else:
        resource_uuid = str(org.id)

    # type must include prov
    has_prov = False
    for t in list(org.type or []):
        for c in list(t.coding or []):
            if str(c.system) == HL7_ORG_TYPE_SYSTEM and str(c.code) == HL7_ORG_TYPE_PROV_CODE:
                has_prov = True
                break
    if not has_prov:
        _warn("Organization.type does not include prov; treating as clinical anyway per current scope")

    # NPI identifier (exactly one)
    npis = []
    for ident in list(org.identifier or []):
        if ident.system and str(ident.system) == US_NPI_SYSTEM and ident.value:
            npis.append(str(ident.value))
    if not npis:
        raise ValueError("ClinicalOrganization missing required NPI")
    if len(npis) > 1:
        report.add("Organization.identifier[npi]", len(npis) - 1)
        _warn(f"Dropping extra NPIs beyond first: {npis[1:]}")
    npi = npis[0]

    # aliases with alias typing from _alias primitive extensions
    aliases_raw = list(org.alias or [])
    alias_exts = list(org.alias__ext or [])
    aliases: list[OrgAlias] = []
    if alias_exts and len(alias_exts) != len(aliases_raw):
        _warn("Organization._alias length does not match alias length; alias typing may be misaligned")

    for i, alias_str in enumerate(aliases_raw):
        alias_type = None
        if i < len(alias_exts):
            ext_container = alias_exts[i]
            for ext in list(ext_container.extension or []):
                if str(ext.url) == ORG_ALIAS_TYPE_EXT_URL:
                    alias_type = (
                        str(ext.valueCode)
                        if ext.valueCode is not None
                        else (str(ext.valueString) if ext.valueString is not None else None)
                    )
        aliases.append(OrgAlias(alias=str(alias_str), alias_type=alias_type))

    # telecom phone/fax only
    telecoms: list[OrgTelecom] = []
    for tp in list(org.telecom or []):
        if tp.system not in ("phone", "fax"):
            report.add("Organization.telecom", 1)
            _warn(f"Dropping unsupported telecom system {tp.system}: {tp.value}")
            continue
        if not tp.value:
            continue
        telecoms.append(OrgTelecom(type=str(tp.system), value=str(tp.value)))

    # addresses (many). Use same canonical Address as practitioner.
    addresses: list[CanonicalAddress] = []
    for addr in list(org.address or []):
        lines = list(addr.line or [])
        if len(lines) > 2:
            report.add("Organization.address.line", len(lines) - 2)
            _warn(f"Dropping extra address lines beyond 2: {lines[2:]}")
            lines = lines[:2]
        addresses.append(
            CanonicalAddress(
                line1=str(lines[0]) if len(lines) >= 1 else None,
                line2=str(lines[1]) if len(lines) >= 2 else None,
                city=str(addr.city) if addr.city else None,
                state=str(addr.state) if addr.state else None,
                postal_code=str(addr.postalCode) if addr.postalCode else None,
                country=str(addr.country) if addr.country else None,
            )
        )

    # partOf
    part_of_resource_uuid = None
    if org.partOf and org.partOf.reference:
        part_of_resource_uuid = str(org.partOf.reference).split("/")[-1]

    # contact: exactly one
    contact_obj = None
    contacts = list(org.contact or [])
    if len(contacts) > 1:
        report.add("Organization.contact", len(contacts) - 1)
        _warn(f"Dropping extra contacts beyond first: {len(contacts) - 1}")
        contacts = contacts[:1]
    if contacts:
        c = contacts[0]
        first_name = None
        last_name = None
        if c.name:
            last_name = str(c.name.family) if c.name.family else None
            given = list(c.name.given or [])
            first_name = str(given[0]) if given else None

        phone = None
        fax = None
        for t in list(c.telecom or []):
            if t.system == "phone" and t.value and phone is None:
                phone = str(t.value)
            elif t.system == "fax" and t.value and fax is None:
                fax = str(t.value)
            elif t.system not in ("phone", "fax"):
                report.add("Organization.contact.telecom", 1)
                _warn(f"Dropping unsupported contact telecom system {t.system}: {t.value}")

        addr = c.address
        if addr is not None:
            lines = list(addr.line or [])
            if len(lines) > 2:
                report.add("Organization.contact.address.line", len(lines) - 2)
                _warn(f"Dropping extra contact address lines beyond 2: {lines[2:]}")
                lines = lines[:2]
            contact_obj = OrgContact(
                first_name=first_name,
                last_name=last_name,
                phone=phone,
                fax=fax,
                address_line1=str(lines[0]) if len(lines) >= 1 else None,
                address_line2=str(lines[1]) if len(lines) >= 2 else None,
                city=str(addr.city) if addr.city else None,
                state=str(addr.state) if addr.state else None,
                postal_code=str(addr.postalCode) if addr.postalCode else None,
                country=str(addr.country) if addr.country else None,
            )
        else:
            contact_obj = OrgContact(
                first_name=first_name,
                last_name=last_name,
                phone=phone,
                fax=fax,
            )

    # endpoints
    endpoints: list[EndpointRef] = []
    for ref in list(org.endpoint or []):
        if ref.reference:
            endpoints.append(EndpointRef(resource_uuid=str(ref.reference).split("/")[-1]))

    # extensions: logo, rating, cms booleans
    logo_url = None
    rating = None
    cms_pecos_validated = None
    cms_ial2_validated = None
    has_cms_aligned_data_network = None

    for ext in list(org.extension or []):
        url = str(ext.url)
        if url == NDH_LOGO_EXT_URL:
            if ext.valueUrl is not None:
                logo_url = str(ext.valueUrl)
            elif ext.valueString is not None:
                logo_url = str(ext.valueString)
        elif url == ORG_RATING_EXT_URL:
            if ext.valueInteger is not None:
                rating = int(ext.valueInteger)
            elif ext.valueString is not None:
                try:
                    rating = int(str(ext.valueString))
                except ValueError:
                    _warn(f"Invalid rating valueString: {ext.valueString}")
            elif ext.valueCode is not None:
                try:
                    rating = int(str(ext.valueCode))
                except ValueError:
                    _warn(f"Invalid rating valueCode: {ext.valueCode}")
        elif url == ORG_CMS_PECOS_VALIDATED_EXT_URL:
            cms_pecos_validated = bool(ext.valueBoolean) if ext.valueBoolean is not None else None
        elif url == ORG_CMS_IAL2_VALIDATED_EXT_URL:
            cms_ial2_validated = bool(ext.valueBoolean) if ext.valueBoolean is not None else None
        elif url == ORG_HAS_CMS_ALIGNED_DATA_NETWORK_EXT_URL:
            has_cms_aligned_data_network = (
                bool(ext.valueBoolean) if ext.valueBoolean is not None else None
            )

    canonical = ClinicalOrganization(
        resource_uuid=resource_uuid,
        npi=npi,
        active=bool(org.active) if org.active is not None else None,
        name=str(org.name) if org.name is not None else None,
        description=str(org.description) if org.description is not None else None,
        logo_url=logo_url,
        rating=rating,
        cms_pecos_validated=cms_pecos_validated,
        cms_ial2_validated=cms_ial2_validated,
        has_cms_aligned_data_network=has_cms_aligned_data_network,
        aliases=aliases,
        telecoms=telecoms,
        addresses=addresses,
        part_of_resource_uuid=part_of_resource_uuid,
        contact=contact_obj,
        endpoints=endpoints,
    )

    return canonical, report


def clinical_organization_to_fhir_json(org: ClinicalOrganization) -> dict[str, Any]:
    """Serialize canonical ClinicalOrganization to FHIR Organization JSON."""

    meta = Meta(profile=[NDH_ORGANIZATION_PROFILE_URL])

    org_type = OrganizationType(
        coding=[
            Coding(
                system=HL7_ORG_TYPE_SYSTEM,
                code=HL7_ORG_TYPE_PROV_CODE,
                display="Healthcare Provider",
            )
        ]
    )

    identifier = [Identifier(system=US_NPI_SYSTEM, value=org.npi)]

    telecom = [ContactPoint(system=t.type, value=t.value) for t in org.telecoms]

    address = [
        Address(
            line=[x for x in [a.line1, a.line2] if x] or None,
            city=a.city,
            state=a.state,
            postalCode=a.postal_code,
            country=a.country,
        )
        for a in org.addresses
    ]

    # aliases + _alias primitive extensions
    alias = [a.alias for a in org.aliases]
    _alias = []
    for a in org.aliases:
        if a.alias_type is None:
            _alias.append(Element())
        else:
            _alias.append(
                Element(
                    extension=[
                        Extension(url=ORG_ALIAS_TYPE_EXT_URL, valueCode=a.alias_type)
                    ]
                )
            )

    part_of = None
    if org.part_of_resource_uuid:
        part_of = Reference(reference=f"Organization/{org.part_of_resource_uuid}")

    contact = None
    if org.contact is not None:
        name = HumanName(
            family=org.contact.last_name,
            given=[x for x in [org.contact.first_name] if x] or None,
        )
        c_telecom = []
        if org.contact.phone:
            c_telecom.append(ContactPoint(system="phone", value=org.contact.phone))
        if org.contact.fax:
            c_telecom.append(ContactPoint(system="fax", value=org.contact.fax))

        c_addr = None
        if any(
            [
                org.contact.address_line1,
                org.contact.address_line2,
                org.contact.city,
                org.contact.state,
                org.contact.postal_code,
                org.contact.country,
            ]
        ):
            c_addr = Address(
                line=[
                    x
                    for x in [org.contact.address_line1, org.contact.address_line2]
                    if x
                ]
                or None,
                city=org.contact.city,
                state=org.contact.state,
                postalCode=org.contact.postal_code,
                country=org.contact.country,
            )

        contact = [OrganizationContact(name=name, telecom=c_telecom or None, address=c_addr)]

    endpoint = [Reference(reference=f"Endpoint/{e.resource_uuid}") for e in org.endpoints]

    extension: list[Extension] = []
    if org.logo_url is not None:
        # Prefer valueUrl for logo.
        extension.append(Extension(url=NDH_LOGO_EXT_URL, valueUrl=org.logo_url))
    if org.rating is not None:
        extension.append(Extension(url=ORG_RATING_EXT_URL, valueInteger=org.rating))

    if org.cms_pecos_validated is not None:
        extension.append(
            Extension(url=ORG_CMS_PECOS_VALIDATED_EXT_URL, valueBoolean=org.cms_pecos_validated)
        )
    if org.cms_ial2_validated is not None:
        extension.append(
            Extension(url=ORG_CMS_IAL2_VALIDATED_EXT_URL, valueBoolean=org.cms_ial2_validated)
        )
    if org.has_cms_aligned_data_network is not None:
        extension.append(
            Extension(
                url=ORG_HAS_CMS_ALIGNED_DATA_NETWORK_EXT_URL,
                valueBoolean=org.has_cms_aligned_data_network,
            )
        )

    fhir = OrganizationResource(
        id=org.resource_uuid,
        meta=meta,
        active=org.active,
        type=[org_type],
        name=org.name,
        alias=alias or None,
        alias__ext=_alias or None,
        description=org.description,
        identifier=identifier,
        telecom=telecom or None,
        address=address or None,
        partOf=part_of,
        contact=contact,
        endpoint=endpoint or None,
        extension=extension or None,
    )

    return fhir.model_dump(by_alias=True, exclude_none=True)
