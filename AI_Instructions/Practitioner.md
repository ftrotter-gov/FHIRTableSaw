## Practitioner Model Definition in the Three-Layer Architecture

Below is a compact implementation-oriented specification for the **Practitioner** resource in the context of the three-layer design.

---

# 1. Purpose

The Practitioner model represents an individual provider as a **canonical domain object** that can move in both directions:

* **FHIR JSON → canonical Practitioner object → relational tables**
* **relational tables → canonical Practitioner object → FHIR JSON**

The Practitioner model is one of the core canonical classes and must support selective flattening for practically singular data, while normalizing shared repeating structures into relational child tables.

---

# 2. Layer Responsibilities

## 2.1 Relational layer

The relational layer stores the practitioner in a compressed, queryable form.

It will contain:

* a primary `practitioner` table
* related child or join tables for repeating and reusable structures
* bridge tables for many-to-many relationships
* supporting lookup or coded tables where appropriate

## 2.2 Canonical domain layer

The canonical layer will define a `Practitioner` class as the semantic center of the model.

This class will contain:

* flattened singular attributes
* references to normalized repeating child objects
* references to related resources
* conversion logic support for both FHIR and relational forms

## 2.3 FHIR layer

The FHIR layer will:

* parse Practitioner JSON into the canonical model
* serialize the canonical model into FHIR Practitioner JSON
* emit extensions and identifiers correctly
* resolve references to related resources such as Endpoint and VerificationResult
* use the endpoint extension to model connections to endpoints in the fhir output here is the link https://build.fhir.org/ig/HL7/fhir-us-ndh/StructureDefinition-base-ext-endpoint-reference.html
* VerificationResult is based on the Verification spec here: https://build.fhir.org/ig/HL7/fhir-us-ndh/StructureDefinition-ndh-Verification.html
* Minimal VerificationResult includes: 'status', attestation (link to Practicioner), validator (link to ValidatingOrganization). Have the reference to ValidatingOrganization but do not implement this now. 
* For communication proficiency use extension https://build.fhir.org/ig/HL7/fhir-us-ndh/StructureDefinition-base-ext-communication-proficiency.html
* Go ahead and implement PractitionerRole.specialty on the Practicioner 
* Implement - `Practitioner.qualification.code` but call it Pracicioner.credential. This will be an array of values, and should be seperate table in the relational model to account for multiple credentials per individual provider. 
* For both of these just assume that these changes will be made to the FHIR IG specification and act as though those changes had already occured. 
* You will need to loosen the pydantic strict fhir.resources validation for practicioner in order to make these last few items work. 
* always hardcode `http://hl7.org/fhir/sid/us-npi` as the identifier system
* make a guess as to the resourceType should be in the VerificationResult and hard code it for now

###

-


-


---

# 3. Canonical Practitioner Object

The canonical `Practitioner` object should include the following fields.

## 3.1 Core identity fields

* `id: string`
* `npi: string`
  Required. This is the primary identifier.
* `active_status: boolean`

## 3.2 Personal name fields

These are flattened onto the practitioner object rather than normalized into a separate name table.

### Legal name

* `first_name`
* `middle_name`
* `last_name`
* `prefix`
* `non_clinical_suffix`

### Alternate name

For the initial version, allow one additional name set as flattened columns rather than a repeating related table.

* `other_first_name`
* `other_middle_name`
* `other_last_name`
* `other_prefix`
* `other_non_clinical_suffix`

## 3.3 Demographic fields

* `gender`
* `race_code`
* `ethnicity_code`

Race and ethnicity must be stored directly on the practitioner table and mapped to the appropriate **US Core race** and **US Core ethnicity** FHIR extensions. These should not have separate relational models in the first version.

## 3.4 Boolean verification-related flags

These are flattened booleans on the practitioner table and represented as FHIR extensions during serialization.

* `is_cms_enrolled: boolean`
* `is_cms_ial2_verified: boolean`
* `is_participating_in_cms_aligned_data_networks: boolean`

These are conceptually part of verification state, but for the relational model they should be simple practitioner attributes.

---

# 4. Practitioner Relationships

The Practitioner object will reference several normalized related structures.

## 4.1 Endpoint relationship

A practitioner may link to **many endpoints**.

Relationally:

* many-to-many relationship between practitioner and endpoint

Canonically:

* `endpoints: list[Endpoint]`

FHIR:

* references to Endpoint resources

## 4.2 Verification relationship

A practitioner may link to **many verification records**.

Relationally:

* many-to-many relationship between practitioner and verification result

Canonically:

* `verification_results: list[VerificationResult]`

FHIR:

* references and/or extensions as required by the target NDH profile

## 4.3 Address relationship

A practitioner may link to **many addresses**.

Relationally:

* many-to-many relationship between practitioner and address

Canonically:

* `addresses: list[Address]`

The address structure should reuse the address portion of the later Location model, but should exist as its own normalized relational object now.

## 4.4 Clinician type relationship

A practitioner may link to **many clinician types**.

Relationally:

* practitioner-to-clinician-type relationship table

Canonically:

* `clinician_types: list[ClinicianType]`

This replaces the earlier phrasing of provider taxonomy or provider type.

## 4.5 Credential relationship

A practitioner may link to **many credentials**.

Relationally:

* practitioner-to-credential relationship table

Canonically:

* `credentials: list[Credential]`

Credentials should be coded and represented through a dedicated relational model.

## 4.6 Language proficiency relationship

A practitioner may link to **many language proficiency records**.

Relationally:

* one-to-many or many-to-many depending on later reuse needs; for now it can be modeled as practitioner-linked rows

Canonically:

* `language_proficiencies: list[LanguageProficiency]`

Each language proficiency record must include:

* coded language
* proficiency level

FHIR serialization should emit these in the appropriate language communication structure or extension structure required by the target profile.

## 4.7 Telecom relationship

Telecom should be normalized into a separate table rather than flattened.

A practitioner may link to **many telecom rows**.

Each telecom row should include at least:

* practitioner foreign key
* type: phone or fax
* value
* optional use or rank later if needed

Canonically:

* `telecoms: list[Telecom]`

For implementation convenience, the canonical model may also expose filtered views:

* `phones`
* `faxes`

---

# 5. Relational Model

## 5.1 Primary practitioner table

Suggested `practitioner` table fields:

* `id`
* `npi`
* `active_status`
* `first_name`
* `middle_name`
* `last_name`
* `prefix`
* `non_clinical_suffix`
* `other_first_name`
* `other_middle_name`
* `other_last_name`
* `other_prefix`
* `other_non_clinical_suffix`
* `gender`
* `race_code`
* `ethnicity_code`
* `is_cms_enrolled`
* `is_cms_ial2_verified`
* `is_participating_in_cms_aligned_data_networks`

## 5.2 Related tables required for Practitioner implementation

To implement Practitioner, the following supporting relational models must also exist:

* `endpoint`
* `practitioner_endpoint`
* `verification_result`
* `practitioner_verification_result`
* `address`
* `practitioner_address`
* `clinician_type`
* `practitioner_clinician_type`
* `credential`
* `practitioner_credential`
* `telecom`
* `language_proficiency`

Depending on future reuse needs, `language_proficiency` could later become more generalized, but it does not need that complexity initially.

---

# 6. Canonical Object Shape

A practical canonical class shape would be conceptually like this:

```python
class Practitioner:
    id: str
    npi: str
    active_status: Boolean

    first_name: str | None
    middle_name: str | None
    last_name: str | None
    prefix: str | None
    non_clinical_suffix: str | None

    other_first_name: str | None
    other_middle_name: str | None
    other_last_name: str | None
    other_prefix: str | None
    other_non_clinical_suffix: str | None

    gender: str | None
    race_code: str | None
    ethnicity_code: str | None

    is_cms_enrolled: bool | None
    is_cms_ial2_verified: bool | None
    is_participating_in_cms_aligned_data_networks: bool | None

    endpoints: list["Endpoint"]
    verification_results: list["VerificationResult"]
    addresses: list["Address"]
    clinician_types: list["ClinicianType"]
    credentials: list["Credential"]
    telecoms: list["Telecom"]
    language_proficiencies: list["LanguageProficiency"]
```

---

# 7. FHIR Mapping Rules

## 7.1 FHIR Practitioner content to support

The FHIR serialization/parsing layer for Practitioner must support at least:

* `id`
* `identifier` for NPI
* `active`
* `name`
* `telecom`
* `address`
* `gender`
* communication/language representation as appropriate
* US Core race extension
* US Core ethnicity extension
* NDH or project-specific verification-related extensions
* references to Endpoint where supported by the implementation pattern

## 7.2 Flattening rules

The following are intentionally flattened in the canonical and relational model:

* legal name
* one alternate name
* race
* ethnicity
* three CMS verification booleans

The following remain normalized:

* endpoints
* verification results
* addresses
* telecom
* clinician types
* credentials
* language proficiencies

## 7.3 NPI handling

NPI is required in the canonical model and must serialize as a FHIR identifier using the correct identifier system.
* npi should be a unique index on the Practicioner tables.


## 7.4 Name handling

FHIR allows multiple names and more complicated name structures.
For the initial system:

* one legal name is mapped to the primary flattened name columns
* one alternate name is mapped to the secondary flattened name columns
* additional FHIR names beyond those two may be ignored, rejected, or placed in an overflow structure depending on implementation policy

## 7.5 Telecom handling

FHIR telecom entries of system `phone` and `fax` map into the normalized telecom table.

For the initial version:

* support phone
* support fax
* other telecom types may be ignored or deferred unless needed later

## 7.6 Race and ethnicity handling

FHIR US Core race and ethnicity extensions must be parsed into:

* `race_code`
* `ethnicity_code`

and serialized back from those columns.

## 7.7 Verification booleans

The three CMS-specific boolean flags must be represented as FHIR extensions on output and parsed from extensions on input.

---

# 8. Required Supporting Canonical Models

The Practitioner specification depends on the existence of at least the following canonical models:

* `Endpoint`
* `VerificationResult`
* `Address`
* `Telecom`
* `ClinicianType`
* `Credential`
* `LanguageProficiency`

Minimal expectations for these:

## Address

A reusable structured address object.

## Telecom

A reusable telecom object with:

* `type` = phone or fax
* `value`

## ClinicianType

A coded classification object for practitioner type.

## Credential

A coded credential object.

## LanguageProficiency

A structured object containing:

* language code or coded concept
* proficiency level

---

# 9. Bidirectional Processing Expectations

## 9.1 FHIR to relational

The ingestion path must:

* parse FHIR Practitioner JSON
* extract flattened singular values
* parse normalized repeating structures
* resolve references to linked resources where needed
* populate practitioner and related tables

## 9.2 Relational to FHIR

The output path must:

* hydrate a canonical Practitioner object from practitioner and related tables
* build valid FHIR Practitioner JSON
* emit extensions correctly
* emit repeating arrays only where appropriate
* preserve the selective flattening strategy

---

# 10. Initial Constraints and Non-Goals

The first Practitioner implementation will not attempt to:

* support unlimited name arrays
* model every possible telecom subtype
* normalize race or ethnicity into separate relational entities
* create separate subclass variants of Practitioner
* represent every possible FHIR Practitioner field in the first release

The focus is a pragmatic, implementable Practitioner model that fits the broader three-layer architecture.

---

# 11. Compact Summary for LLM Code Generation

The Practitioner model should be implemented as a canonical class in a three-layer architecture. It must support bidirectional conversion between FHIR JSON and a compressed relational schema. The main practitioner table should contain flattened singular fields for ID, NPI, active status, legal name, one alternate name, gender, race, ethnicity, and three CMS verification booleans. Repeating or reusable structures must be normalized into related models and tables, including endpoints, verification results, addresses, telecom, clinician types, credentials, and language proficiencies. Telecom should use a single table with a type field for phone versus fax. Race and ethnicity should be stored directly on the practitioner table and mapped to US Core extensions. The three CMS verification flags should be stored as boolean columns and mapped to FHIR extensions. The implementation must support both FHIR-to-relational ingestion and relational-to-FHIR serialization using the same canonical Practitioner object.


