"""Minimal FHIR R4 models built on top of fhir-core.

We intentionally model *only* the parts of the FHIR resources required for the
currently discussed profile (Practitioner).

Important: fhir-core's serializer expects that if an element's
`elements_sequence()` includes `id` and/or `extension`, then those fields must
exist on the model. To keep things consistent, we provide minimal `Element` and
`Resource` bases and subclass from them.
"""

from __future__ import annotations

from typing import Literal

from fhir_core.fhirabstractmodel import FHIRAbstractModel
from fhir_core.types import BooleanType, CodeType, IdType, IntegerType, StringType
from pydantic import ConfigDict, Field


class Element(FHIRAbstractModel):
    __resource_type__ = "Element"

    # Real-world NDH servers include many fields we don't model.
    # For the 3-tier pipeline, we intentionally parse only a small subset,
    # so we ignore unknown fields instead of failing validation.
    model_config = ConfigDict(extra="ignore")

    id: IdType | None = Field(None, alias="id", json_schema_extra={"element_property": True})
    extension: list["Extension"] | None = Field(
        None, alias="extension", json_schema_extra={"element_property": True}
    )

    @classmethod
    def elements_sequence(cls) -> list[str]:
        return ["id", "extension"]


class Resource(Element):
    """Minimal base Resource.

    fhir-core checks for a class named `Resource` in the MRO to decide whether to
    emit `resourceType`.
    """

    __resource_type__ = "Resource"


class Coding(Element):
    __resource_type__ = "Coding"

    # Use StringType rather than UrlType because real-world servers sometimes
    # include non-URL system values (e.g., urn:oid) and we don't want strict URL
    # validation to break ingestion.
    system: StringType | None = Field(
        None, alias="system", json_schema_extra={"element_property": True}
    )
    code: CodeType | None = Field(
        None, alias="code", json_schema_extra={"element_property": True}
    )
    display: StringType | None = Field(
        None, alias="display", json_schema_extra={"element_property": True}
    )

    @classmethod
    def elements_sequence(cls) -> list[str]:
        return ["id", "extension", "system", "code", "display"]


class CodeableConcept(Element):
    __resource_type__ = "CodeableConcept"

    coding: list[Coding] | None = Field(
        None, alias="coding", json_schema_extra={"element_property": True}
    )
    text: StringType | None = Field(
        None, alias="text", json_schema_extra={"element_property": True}
    )

    @classmethod
    def elements_sequence(cls) -> list[str]:
        return ["id", "extension", "coding", "text"]


class Reference(Element):
    __resource_type__ = "Reference"

    reference: StringType | None = Field(
        None, alias="reference", json_schema_extra={"element_property": True}
    )
    display: StringType | None = Field(
        None, alias="display", json_schema_extra={"element_property": True}
    )

    @classmethod
    def elements_sequence(cls) -> list[str]:
        return ["id", "extension", "reference", "display"]


class Extension(Element):
    __resource_type__ = "Extension"

    url: StringType = Field(..., alias="url", json_schema_extra={"element_property": True})

    # Support only what we need right now.
    valueBoolean: BooleanType | None = Field(
        None, alias="valueBoolean", json_schema_extra={"element_property": True}
    )
    valueInteger: IntegerType | None = Field(
        None, alias="valueInteger", json_schema_extra={"element_property": True}
    )
    valueCode: CodeType | None = Field(
        None, alias="valueCode", json_schema_extra={"element_property": True}
    )
    valueString: StringType | None = Field(
        None, alias="valueString", json_schema_extra={"element_property": True}
    )
    valueUrl: StringType | None = Field(
        None, alias="valueUrl", json_schema_extra={"element_property": True}
    )
    valueCoding: Coding | None = Field(
        None, alias="valueCoding", json_schema_extra={"element_property": True}
    )
    valueReference: Reference | None = Field(
        None, alias="valueReference", json_schema_extra={"element_property": True}
    )

    @classmethod
    def elements_sequence(cls) -> list[str]:
        return [
            "id",
            "extension",
            "url",
            "valueBoolean",
            "valueInteger",
            "valueCode",
            "valueString",
            "valueUrl",
            "valueCoding",
            "valueReference",
        ]


class Identifier(Element):
    __resource_type__ = "Identifier"

    system: StringType | None = Field(
        None, alias="system", json_schema_extra={"element_property": True}
    )
    value: StringType | None = Field(
        None, alias="value", json_schema_extra={"element_property": True}
    )

    @classmethod
    def elements_sequence(cls) -> list[str]:
        return ["id", "extension", "system", "value"]


class HumanName(Element):
    __resource_type__ = "HumanName"

    family: StringType | None = Field(
        None, alias="family", json_schema_extra={"element_property": True}
    )
    given: list[StringType] | None = Field(
        None, alias="given", json_schema_extra={"element_property": True}
    )
    prefix: list[StringType] | None = Field(
        None, alias="prefix", json_schema_extra={"element_property": True}
    )
    suffix: list[StringType] | None = Field(
        None, alias="suffix", json_schema_extra={"element_property": True}
    )

    @classmethod
    def elements_sequence(cls) -> list[str]:
        return ["id", "extension", "family", "given", "prefix", "suffix"]


ContactPointSystem = Literal["phone", "fax", "email", "url", "pager", "sms", "other"]


class ContactPoint(Element):
    __resource_type__ = "ContactPoint"

    system: ContactPointSystem | None = Field(
        None, alias="system", json_schema_extra={"element_property": True}
    )
    value: StringType | None = Field(
        None, alias="value", json_schema_extra={"element_property": True}
    )

    @classmethod
    def elements_sequence(cls) -> list[str]:
        return ["id", "extension", "system", "value"]


class Address(Element):
    __resource_type__ = "Address"

    line: list[StringType] | None = Field(
        None, alias="line", json_schema_extra={"element_property": True}
    )
    city: StringType | None = Field(
        None, alias="city", json_schema_extra={"element_property": True}
    )
    state: StringType | None = Field(
        None, alias="state", json_schema_extra={"element_property": True}
    )
    postalCode: StringType | None = Field(
        None, alias="postalCode", json_schema_extra={"element_property": True}
    )
    country: StringType | None = Field(
        None, alias="country", json_schema_extra={"element_property": True}
    )

    @classmethod
    def elements_sequence(cls) -> list[str]:
        return ["id", "extension", "line", "city", "state", "postalCode", "country"]


class PractitionerCommunication(Element):
    __resource_type__ = "PractitionerCommunication"

    language: CodeableConcept | None = Field(
        None, alias="language", json_schema_extra={"element_property": True}
    )

    @classmethod
    def elements_sequence(cls) -> list[str]:
        return ["id", "extension", "language"]


class PractitionerQualification(Element):
    __resource_type__ = "PractitionerQualification"

    code: CodeableConcept | None = Field(
        None, alias="code", json_schema_extra={"element_property": True}
    )

    @classmethod
    def elements_sequence(cls) -> list[str]:
        return ["id", "extension", "code"]


class PractitionerResource(Resource):
    __resource_type__ = "Practitioner"

    identifier: list[Identifier] | None = Field(
        None, alias="identifier", json_schema_extra={"element_property": True}
    )
    active: BooleanType | None = Field(
        None, alias="active", json_schema_extra={"element_property": True}
    )
    name: list[HumanName] | None = Field(
        None, alias="name", json_schema_extra={"element_property": True}
    )
    telecom: list[ContactPoint] | None = Field(
        None, alias="telecom", json_schema_extra={"element_property": True}
    )
    address: list[Address] | None = Field(
        None, alias="address", json_schema_extra={"element_property": True}
    )
    gender: CodeType | None = Field(
        None, alias="gender", json_schema_extra={"element_property": True}
    )
    communication: list[PractitionerCommunication] | None = Field(
        None, alias="communication", json_schema_extra={"element_property": True}
    )
    qualification: list[PractitionerQualification] | None = Field(
        None, alias="qualification", json_schema_extra={"element_property": True}
    )

    # Instruction: "assume IG changes already occurred"
    specialty: list[CodeableConcept] | None = Field(
        None, alias="specialty", json_schema_extra={"element_property": True}
    )

    @classmethod
    def elements_sequence(cls) -> list[str]:
        return [
            "id",
            "extension",
            "identifier",
            "active",
            "name",
            "telecom",
            "address",
            "gender",
            "communication",
            "qualification",
            "specialty",
        ]


# --- PractitionerRole (minimal) ---


class PractitionerRoleResource(Resource):
    __resource_type__ = "PractitionerRole"

    active: BooleanType | None = Field(
        None, alias="active", json_schema_extra={"element_property": True}
    )

    practitioner: Reference | None = Field(
        None, alias="practitioner", json_schema_extra={"element_property": True}
    )
    organization: Reference | None = Field(
        None, alias="organization", json_schema_extra={"element_property": True}
    )

    code: list[CodeableConcept] | None = Field(
        None, alias="code", json_schema_extra={"element_property": True}
    )
    specialty: list[CodeableConcept] | None = Field(
        None, alias="specialty", json_schema_extra={"element_property": True}
    )

    telecom: list[ContactPoint] | None = Field(
        None, alias="telecom", json_schema_extra={"element_property": True}
    )

    endpoint: list[Reference] | None = Field(
        None, alias="endpoint", json_schema_extra={"element_property": True}
    )

    location: list[Reference] | None = Field(
        None, alias="location", json_schema_extra={"element_property": True}
    )

    healthcareService: list[Reference] | None = Field(
        None, alias="healthcareService", json_schema_extra={"element_property": True}
    )

    @classmethod
    def elements_sequence(cls) -> list[str]:
        return [
            "id",
            "extension",
            "active",
            "practitioner",
            "organization",
            "code",
            "specialty",
            "telecom",
            "endpoint",
            "location",
            "healthcareService",
        ]


# --- Organization (minimal) ---


class Meta(Element):
    __resource_type__ = "Meta"

    profile: list[StringType] | None = Field(
        None, alias="profile", json_schema_extra={"element_property": True}
    )

    @classmethod
    def elements_sequence(cls) -> list[str]:
        return ["id", "extension", "profile"]


class OrganizationType(Element):
    __resource_type__ = "OrganizationType"

    coding: list[Coding] | None = Field(
        None, alias="coding", json_schema_extra={"element_property": True}
    )

    @classmethod
    def elements_sequence(cls) -> list[str]:
        return ["id", "extension", "coding"]


class OrganizationContact(Element):
    __resource_type__ = "OrganizationContact"

    name: HumanName | None = Field(
        None, alias="name", json_schema_extra={"element_property": True}
    )
    telecom: list[ContactPoint] | None = Field(
        None, alias="telecom", json_schema_extra={"element_property": True}
    )
    address: Address | None = Field(
        None, alias="address", json_schema_extra={"element_property": True}
    )

    @classmethod
    def elements_sequence(cls) -> list[str]:
        return ["id", "extension", "name", "telecom", "address"]


class OrganizationResource(Resource):
    __resource_type__ = "Organization"

    meta: Meta | None = Field(None, alias="meta", json_schema_extra={"element_property": True})

    active: BooleanType | None = Field(
        None, alias="active", json_schema_extra={"element_property": True}
    )
    name: StringType | None = Field(
        None, alias="name", json_schema_extra={"element_property": True}
    )
    alias: list[StringType] | None = Field(
        None, alias="alias", json_schema_extra={"element_property": True}
    )
    alias__ext: list[Element] | None = Field(
        None, alias="_alias", json_schema_extra={"element_property": True}
    )
    description: StringType | None = Field(
        None, alias="description", json_schema_extra={"element_property": True}
    )

    identifier: list[Identifier] | None = Field(
        None, alias="identifier", json_schema_extra={"element_property": True}
    )
    telecom: list[ContactPoint] | None = Field(
        None, alias="telecom", json_schema_extra={"element_property": True}
    )
    address: list[Address] | None = Field(
        None, alias="address", json_schema_extra={"element_property": True}
    )

    partOf: Reference | None = Field(
        None, alias="partOf", json_schema_extra={"element_property": True}
    )
    contact: list[OrganizationContact] | None = Field(
        None, alias="contact", json_schema_extra={"element_property": True}
    )

    endpoint: list[Reference] | None = Field(
        None, alias="endpoint", json_schema_extra={"element_property": True}
    )

    type: list[OrganizationType] | None = Field(
        None, alias="type", json_schema_extra={"element_property": True}
    )

    @classmethod
    def elements_sequence(cls) -> list[str]:
        return [
            "id",
            "meta",
            "extension",
            "active",
            "type",
            "name",
            "alias",
            "_alias",
            "description",
            "identifier",
            "telecom",
            "address",
            "partOf",
            "contact",
            "endpoint",
        ]

    @classmethod
    def get_alias_mapping(cls) -> dict[str, str]:
        # Override the base alias mapping so that the JSON property `_alias` maps to
        # our python field name `alias__ext`.
        mapping = super().get_alias_mapping()
        mapping["_alias"] = "alias__ext"
        return mapping


# --- OrganizationAffiliation (minimal) ---


class OrganizationAffiliationResource(Resource):
    __resource_type__ = "OrganizationAffiliation"

    active: BooleanType | None = Field(
        None, alias="active", json_schema_extra={"element_property": True}
    )

    organization: Reference | None = Field(
        None, alias="organization", json_schema_extra={"element_property": True}
    )
    participatingOrganization: Reference | None = Field(
        None,
        alias="participatingOrganization",
        json_schema_extra={"element_property": True},
    )

    code: list[CodeableConcept] | None = Field(
        None, alias="code", json_schema_extra={"element_property": True}
    )
    specialty: list[CodeableConcept] | None = Field(
        None, alias="specialty", json_schema_extra={"element_property": True}
    )
    telecom: list[ContactPoint] | None = Field(
        None, alias="telecom", json_schema_extra={"element_property": True}
    )

    # Explicitly exclude location/healthcareService for now (not modeled).

    @classmethod
    def elements_sequence(cls) -> list[str]:
        return [
            "id",
            "extension",
            "active",
            "organization",
            "participatingOrganization",
            "code",
            "specialty",
            "telecom",
        ]
