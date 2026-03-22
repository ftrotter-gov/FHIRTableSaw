# FIRE Resource Description: Endpoint

The definition for this lives here: https://build.fhir.org/ig/HL7/fhir-us-ndh/StructureDefinition-ndh-Endpoint.html

## Purpose

The **Endpoint** resource represents a technical connection point for exchanging information. In this architecture, Endpoint is used to describe a reachable interoperability interface associated with selected directory entities.

Although the base FHIR model allows Endpoint to be associated with many resource types, this implementation will only support relationships from Endpoint to:

* **Practitioner**
* **PractitionerRole**
* **Organization**
* **OrganizationAffiliation**

## Core modeling notes

* Endpoint includes an **endpoint rank** extension.
* Endpoint has a **status**.
* Endpoint has a **connection type**.
* Endpoint has a **name**.
* Endpoint has one or more **payload types**.
* Endpoint may be linked to only the supported target resource types listed above.
* Relationship tracking should be explicitly modeled for those supported types only.

## Cardinality summary

* **endpoint_rank**: `0..1`
* **status**: `1..1`
* **connection_type**: `1..1`
* **name**: `0..1`
* **payload_type**: `0..1`
* **practitioner link(s)**: `0..*`
* **practitioner_role link(s)**: `0..*`
* **organization link(s)**: `0..*`
* **organization_affiliation link(s)**: `0..*`

---

# Logical definition

## Table: `endpoint`

### Primary key

* `id`

  * Type: BIGINT
  * Cardinality: `1..1`
  * Description: Primary key for the endpoint record.

* resource_uuid

  * Type: uuid
  * Cardinality: `1..1`
  * Description: FHIR key for the endpoint record.


### Fields

* `status`

  * Type: code
  * Cardinality: `1..1`
  * Description: Operational state of the endpoint.
  * Notes:

    * This should reflect the FHIR Endpoint status value set.
    * Typical values include active, suspended, error, off, entered-in-error, or test depending on version/profile usage.

* `connection_type`

  * Type: coded concept
  * Cardinality: `1..1`
  * Description: The technical protocol or connection mechanism used by the endpoint.

* `name`

  * Type: string, nullable
  * Cardinality: `0..1`
  * Description: Optional human-readable name for the endpoint.

* `endpoint_rank`

  * Type: integer, nullable
  * Cardinality: `0..1`
  * Description: Extension representing endpoint rank or preference ordering.

### Notes on omitted fields

You only listed the following core fields for this specification:

* endpoint rank extension
* status
* connection type
* name
* payload type

So this resource definition intentionally excludes other standard Endpoint attributes unless you later decide to add them.

---

# Repeating coded concepts

## Table: `endpoint_payload_type`

This table stores the repeating payload type values for an endpoint.

### Purpose

Represents one or more payload types supported by the endpoint.

### Fields

* `id`

  * Type: UUID
  * Cardinality: `1..1`
  * Description: Primary key.

* `endpoint_id`

  * Type: UUID
  * Cardinality: `1..1`
  * References: `endpoint(id)`
  * Description: Parent endpoint.

* `payload_type`

  * Type: coded concept
  * Cardinality: `1..1`
  * Description: Payload type supported by the endpoint.

### Constraint

* Each endpoint must have **at least one** payload type.

---

# Relationship tables

Because Endpoint may relate to multiple supported resource types, and because you only want those specific types tracked, the cleanest relational design is to use explicit join tables for each allowed target type.

This keeps the bidirectional model clear and prevents unsupported link targets.

## Table: `endpoint_practitioner`

### Purpose

Associates an endpoint with one or more practitioners.

### Fields

* `id`

  * Type: UUID
  * Cardinality: `1..1`

* `endpoint_id`

  * Type: UUID
  * Cardinality: `1..1`
  * References: `endpoint(id)`

* `practitioner_id`

  * Type: UUID
  * Cardinality: `1..1`
  * References: `practitioner(id)`

### Constraint

* Unique on `(endpoint_id, practitioner_id)`

---

## Table: `endpoint_practitioner_role`

### Purpose

Associates an endpoint with one or more practitioner roles.

### Fields

* `id`

  * Type: UUID
  * Cardinality: `1..1`

* `endpoint_id`

  * Type: UUID
  * Cardinality: `1..1`
  * References: `endpoint(id)`

* `practitioner_role_id`

  * Type: UUID
  * Cardinality: `1..1`
  * References: `practitioner_role(id)`

### Constraint

* Unique on `(endpoint_id, practitioner_role_id)`

---

## Table: `endpoint_organization`

### Purpose

Associates an endpoint with one or more organizations.

### Fields

* `id`

  * Type: UUID
  * Cardinality: `1..1`

* `endpoint_id`

  * Type: UUID
  * Cardinality: `1..1`
  * References: `endpoint(id)`

* `organization_id`

  * Type: UUID
  * Cardinality: `1..1`
  * References: `organization(id)`

### Constraint

* Unique on `(endpoint_id, organization_id)`

---

## Table: `endpoint_organization_affiliation`

### Purpose

Associates an endpoint with one or more organization affiliations.

### Fields

* `id`

  * Type: UUID
  * Cardinality: `1..1`

* `endpoint_id`

  * Type: UUID
  * Cardinality: `1..1`
  * References: `endpoint(id)`

* `organization_affiliation_id`

  * Type: UUID
  * Cardinality: `1..1`
  * References: `organization_affiliation(id)`

### Constraint

* Unique on `(endpoint_id, organization_affiliation_id)`

---

# Relationship summary

## Outbound relationships from Endpoint

* `endpoint_payload_type.endpoint_id -> endpoint.id`
* `endpoint_practitioner.endpoint_id -> endpoint.id`
* `endpoint_practitioner_role.endpoint_id -> endpoint.id`
* `endpoint_organization.endpoint_id -> endpoint.id`
* `endpoint_organization_affiliation.endpoint_id -> endpoint.id`

## Supported inbound link targets

* `endpoint_practitioner.practitioner_id -> practitioner.id`
* `endpoint_practitioner_role.practitioner_role_id -> practitioner_role.id`
* `endpoint_organization.organization_id -> organization.id`
* `endpoint_organization_affiliation.organization_affiliation_id -> organization_affiliation.id`

## Bidirectional architecture considerations

To support bidirectional traversal under the NDH-style architecture:

* From **Practitioner**, resolve linked endpoints through:

  * `endpoint_practitioner.practitioner_id = practitioner.id`

* From **PractitionerRole**, resolve linked endpoints through:

  * `endpoint_practitioner_role.practitioner_role_id = practitioner_role.id`

* From **Organization**, resolve linked endpoints through:

  * `endpoint_organization.organization_id = organization.id`

* From **OrganizationAffiliation**, resolve linked endpoints through:

  * `endpoint_organization_affiliation.organization_affiliation_id = organization_affiliation.id`

* From **Endpoint**, resolve all related resources through the appropriate join tables.

---

# Recommended PostgreSQL shape

## `endpoint`

* `id uuid primary key`
* `status text not null`
* `connection_type ... not null`
* `name text null`
* `endpoint_rank integer null`

## `endpoint_payload_type`

* `id uuid primary key`
* `endpoint_id uuid not null`
* `payload_type ... not null`

## `endpoint_practitioner`

* `id uuid primary key`
* `endpoint_id uuid not null`
* `practitioner_id uuid not null`
* unique `(endpoint_id, practitioner_id)`

## `endpoint_practitioner_role`

* `id uuid primary key`
* `endpoint_id uuid not null`
* `practitioner_role_id uuid not null`
* unique `(endpoint_id, practitioner_role_id)`

## `endpoint_organization`

* `id uuid primary key`
* `endpoint_id uuid not null`
* `organization_id uuid not null`
* unique `(endpoint_id, organization_id)`

## `endpoint_organization_affiliation`

* `id uuid primary key`
* `endpoint_id uuid not null`
* `organization_affiliation_id uuid not null`
* unique `(endpoint_id, organization_affiliation_id)`

---

# Constraints and business rules

* Each endpoint must have exactly one status.
* Each endpoint must have exactly one connection type.
* Each endpoint may have zero or one name.
* Each endpoint may have zero or one endpoint rank.
* Each endpoint must have one or more payload types.
* Endpoints may be linked only to:

  * practitioner
  * practitioner_role
  * organization
  * organization_affiliation
* No relational link tables should be created for any other FHIR resource targets for this implementation.
* Duplicate links within a join table must be prevented with unique constraints.

---

# Implementation notes

## Endpoint rank

Because endpoint rank is an extension, you have two reasonable implementation choices:

### Option A: flatten it

Store it directly as:

* `endpoint_rank integer null`

This is the simplest and probably the best fit if rank is a stable, single-valued extension in your model.

### Option B: extension framework

If you are maintaining a generalized extension pattern across FIRE resources, endpoint rank could instead be represented through that shared extension model. But for this resource, flattening it is likely cleaner.

## Coded concept handling

For:

* `connection_type`
* `payload_type`

you should use the same normalized coded concept pattern you have used elsewhere in FIRE, rather than reducing them to plain strings.

---

# Suggested concise FIRE-style summary

## Resource: Endpoint

Represents a technical interoperability endpoint associated with selected directory entities.

## Scalar fields

* `status` — required
* `connection_type` — required
* `name` — optional
* `endpoint_rank` — optional extension, flattened as integer

## Repeating fields

* `payload_type` — one or more coded concepts

## Allowed relationship targets

* Practitioner
* PractitionerRole
* Organization
* OrganizationAffiliation

## Excluded relationship targets

* All other FHIR-permitted Endpoint associations are out of scope for this implementation.

If you want, I can also convert this into the stricter **field-by-field tabular spec format** with columns like:

**field name | datatype | cardinality | nullable | foreign key | description | implementation note**

to match the exact format from the rest of your FIRE/Table Saw documents.
