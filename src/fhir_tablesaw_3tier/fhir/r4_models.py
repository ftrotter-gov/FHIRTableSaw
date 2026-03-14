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
from fhir_core.types import BooleanType, CodeType, IdType, StringType, UrlType
from pydantic import Field


class Element(FHIRAbstractModel):
    __resource_type__ = "Element"

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

    system: UrlType | None = Field(
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

    url: UrlType = Field(..., alias="url", json_schema_extra={"element_property": True})

    # Support only what we need right now.
    valueBoolean: BooleanType | None = Field(
        None, alias="valueBoolean", json_schema_extra={"element_property": True}
    )
    valueCode: CodeType | None = Field(
        None, alias="valueCode", json_schema_extra={"element_property": True}
    )
    valueString: StringType | None = Field(
        None, alias="valueString", json_schema_extra={"element_property": True}
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
            "valueCode",
            "valueString",
            "valueCoding",
            "valueReference",
        ]


class Identifier(Element):
    __resource_type__ = "Identifier"

    system: UrlType | None = Field(
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
