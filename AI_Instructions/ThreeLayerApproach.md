## Compact Software Specification: NDH/FHIR Relational Bridge

### 1. Purpose

This system will provide a **bidirectional bridge** between:

* a **compressed relational model** for NDH-relevant healthcare directory data, and
* **FHIR JSON** that conforms to the National Directory of Healthcare Interoperability (NDH) style of resource exchange.

The bridge must support both directions:

* **FHIR JSON → relational tables**
* **relational tables → FHIR JSON**

The design must preserve enough structure to produce compliant FHIR output while avoiding unnecessary complexity in the internal relational model.

There is a previous project, which should remain seperate in ./src/fhir_tablesaw
Please place all code from this project in ./src/fhir_tablesaw_3tier. Do not reference or co-mingle with sourcecode from the ./src/fhir_tablesaw directory.

---

### 2. Architectural Principle

The system will use **three layers**:

1. **Relational layer**
   Durable storage, querying, normalization, and compression.

2. **Canonical domain model layer**
   Python classes representing the real semantic entities used by the application. The details for these will be specified one at a time, in seperate markdown files.

3. **FHIR serialization/parsing layer**
   Logic for converting between canonical objects and FHIR JSON.

This means the system will not map directly from SQL to FHIR or from FHIR directly into tables. Instead, it will use a canonical in-memory model as the semantic center.

Transformation paths:

* **FHIR JSON → canonical model → relational**
* **relational → canonical model → FHIR JSON**

---

### 3. Core Design Decisions

#### 3.1 Canonical object model

The Python class model is the central representation of the system. These classes are not merely FHIR output wrappers. They are the internal semantic representation of the supported NDH concepts.

Each major canonical class must support the following conceptual operations:

* construct from FHIR input
* serialize to FHIR output
* construct from relational data
* flatten to relational data

#### 3.2 Organization subclassing

A single base organizational class will be used, with subclasses for meaningful NDH-specific organization roles.

Initial organization hierarchy:

* `Organization` (base)
* `ClinicalOrganization`
* `VerifyingOrganization`
* `PayerOrganization`

Additional organization subclasses may be added later as needed.

Subclassing is required because these organization types may differ in:

* validation rules
* expected identifiers
* meaningful fields
* applicable extensions
* FHIR serialization behavior

#### 3.3 Initial major resource classes

The first major canonical resource classes will be:

1. Organization
2. ClinicalOrganization
3. VerifyingOrganization
4. PayerOrganization
5. Practitioner
6. PractitionerRole
7. Location
8. Endpoint
9. OrganizationAffiliation
10. VerificationResult

A bundle/container abstraction will also be required for grouped FHIR exchange, but the primary focus is on the ten major resource classes above.

---

### 4. Intelligent Flattening Strategy

A central design goal is **intelligent flattening**.

FHIR allows deep nesting, repeating substructures, and optional arrays in many places. In practice, many of those structures are used in a much narrower way. The internal model should therefore flatten those structures whenever the practical cardinality is effectively:

* zero
* one
* never more than one in the intended use case

The system should not create distinct subclasses or separate relational structures for every possible FHIR nesting pattern when the real-world use case does not require them.

#### 4.1 Flatten when

Flatten a field into the main resource model when:

* the value is semantically singular in practice
* repeated cardinality is theoretically allowed by FHIR but not needed in the intended NDH implementation
* separate normalization would add complexity without business value

#### 4.2 Normalize when

Normalize into related relational tables when the pattern is broadly reused, truly repeating, or shared across multiple resource types.

Initial normalized repeating patterns should include at least:

* identifiers
* telecom
* addresses
* specialties

Other repeating cross-resource structures may be normalized later as needed.

---

### 5. Canonical Model Requirements

#### 5.1 Resource identity

Each canonical object must support distinct identity concepts where applicable:

* internal relational primary key
* business identifier(s)
* FHIR resource identifier
* references to other canonical objects

The canonical layer must represent relationships semantically, not merely as raw FHIR reference strings.

#### 5.2 Reuse across directions

The same canonical class structure must be usable in both directions:

* ingesting FHIR into relational form
* generating FHIR from relational form

This avoids maintaining separate one-way parsers and output builders.

#### 5.3 Partial fidelity support

Where practical, the canonical model may retain unmapped or profile-specific content in controlled extension or overflow structures so that the system can preserve important content that has not yet been fully normalized.

---

### 6. Relational Model Requirements

The relational model should represent stable business entities and normalized repeating patterns, not every detail of the FHIR document structure.

The relational schema should prioritize:

* compression
* clarity
* queryability
* consistency
* support for round-tripping through the canonical layer

The schema should model major entities directly and normalize shared repeating structures separately.

It should avoid mirroring FHIR purely for the sake of structural fidelity.

---

### 7. FHIR Layer Requirements

The FHIR layer is responsible for:

* parsing FHIR JSON into canonical objects
* resolving references across related resources
* serializing canonical objects into valid FHIR JSON
* packaging grouped resources into FHIR bundles as needed

This layer must understand NDH-specific usage patterns and profile expectations.

The FHIR layer should treat FHIR as an **interchange format**, not as the internal source of truth.

---

### 8. Validation Requirements

Validation must occur primarily in the canonical layer, with support from the FHIR and relational layers as appropriate.

Validation categories include:

* structural FHIR validation
* NDH/profile-specific validation
* relational integrity validation

Organization subclasses must be allowed to enforce subtype-specific validation rules.

---

### 9. Initial Scope

The initial implementation should focus on full bidirectional support for the following resource families:

* Organization and its initial subclasses
* Practitioner
* PractitionerRole
* Location
* Endpoint
* OrganizationAffiliation
* VerificationResult

The first milestone is a working round-trip architecture that proves:

* FHIR JSON can be ingested into canonical objects and persisted relationally
* relational data can be reconstituted into canonical objects and emitted as FHIR JSON

---

### 10. Non-Goals for the Initial Version

The initial version will not attempt to:

* represent every legal FHIR nesting pattern as a separate class
* support every possible NDH resource profile at once
* preserve all FHIR structures in a one-to-one relational mirror
* model every possible repeating subarray when real-world usage is effectively singular

The focus is a pragmatic, bidirectional, maintainable NDH/FHIR bridge with selective flattening and targeted normalization.

---


### Implementation details

* All code should target Postgresql through SQLAlchemy. 
* use fhir.resources and take a pydantic approach as appropriate. 
* Please use serial bigint for all internal keys, but there should be parallel uuid fields for all objects that require a FHIR id. Call the uuid field resource_uuid.
* Use the surrogate "id" for all tables. So PracticionerRole.id and not PracticionerRole.PracticionerRole_id. 
* Do not implement Foreign Keys.
* For the time being assume that all extension urls are https://example.com/extension_url/
* use the standard urls for race and ethnicity, we will define a url to source the CDC codeset in the future. For now please just use placeholders of "purple" and "orange" as hardcoded values.
* The fromFhir function should accept the url of a FHIR server in order to support the automatic lookup of codesets and referred objects as needed
* Pleae only implement whichever FHIR Profile is being discussed, one at a time. This will require the implementation of multiple underlying relational tables. Do not "look ahead" to implement other profiles in advance. 
* Use the nucc codes for clinician_type and for now just a string for credential. We will implement both of this via API shortly. 
* When implementing from_fhir, if the FHIR josn has more repeasting columns than are hardcoded into the flattened relational structure.. ignore them buy print a warning with the dropped values... generate a report at the end of the from_fhir function that details what specific array types were dropped this way most often. Please just print this to stdout after the script runs.
* Target FHIR R4
* Servers for NDH do not require authentication, so simply knowing the URL will allow for querying using GET as needed




###  Summary

This system will use a **three-layer architecture** centered on a **canonical Python object model**. It will support **bidirectional conversion** between a **compressed relational schema** and **FHIR JSON**. The design will use a **base organization class with subclasses** for major NDH organization roles, begin with **ten core resource classes**, and apply **intelligent flattening** to FHIR structures that are theoretically repetitive but practically singular. Shared repeating patterns such as identifiers, telecom, addresses, and specialties will be normalized separately.

