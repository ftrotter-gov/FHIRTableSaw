

# FHIR Resource Description: Location

## Purpose

The **Location** resource represents a physical place where services are delivered or associated. It supports hierarchical composition, organizational management, geographic boundary definition, accessibility characterization, and geospatial coordinates.

## Core modeling notes

* A **Location** has **one and only one address**.
* A **Location** may be **part of another Location** through a self-reference.
* A **Location** may have **one managing organization**.
* A **Location** has **exactly one boundary GeoJSON object**, stored as a single GeoJSON text/JSON field.
* A **Location** may have **zero or more accessibility extensions**, represented as an array of coded concepts.
* Geographic point data is stored directly as **latitude**, **longitude**, and **altitude** fields.

## Cardinality summary

* **boundary_geojson**: `1..1`
* **accessibility_extensions**: `0..*`
* **active**: `1..1`
* **name**: `1..1`
* **address_id**: `1..1`
* **managing_organization_id**: `0..1`
* **part_of_location_id**: `0..1`
* **description**: `0..1`
* **latitude**: `0..1`
* **longitude**: `0..1`
* **altitude**: `0..1`
* **availability_exceptions**: `0..1`

---

# Logical definition

## Table: `location`

### Primary key

* `id`

  * Type: UUID
  * Cardinality: `1..1`
  * Description: Primary key for the location record.

### Fields

* `active`

  * Type: boolean
  * Cardinality: `1..1`
  * Description: Indicates whether the location is active or inactive.

* `name`

  * Type: string
  * Cardinality: `1..1`
  * Description: Human-readable name of the location.

* `description`

  * Type: string, nullable
  * Cardinality: `0..1`
  * Description: Optional free-text description of the location.

* `boundary_geojson`

  * Type: JSON / JSONB
  * Cardinality: `1..1`
  * Description: A single GeoJSON object representing the geographic boundary of the location.
  * Notes:

    * Only one boundary object is allowed per location.
    * In PostgreSQL, `jsonb` is preferred unless there is a specific reason to preserve literal input formatting.

* `latitude`

  * Type: numeric
  * Cardinality: `0..1`
  * Description: Latitude coordinate for the location.

* `longitude`

  * Type: numeric
  * Cardinality: `0..1`
  * Description: Longitude coordinate for the location.

* `altitude`

  * Type: numeric
  * Cardinality: `0..1`
  * Description: Altitude for the location, if known.

* `availability_exceptions`

  * Type: string, nullable
  * Cardinality: `0..1`
  * Description: Free-text description of exceptions to normal availability.

### Foreign keys

* `address_id`

  * Type: UUID
  * Cardinality: `1..1`
  * References: `address(id)`
  * Description: The single address associated with this location.

* `managing_organization_id`

  * Type: UUID, nullable
  * Cardinality: `0..1`
  * References: `organization(id)`
  * Description: Organization responsible for managing the location.

* `part_of_location_id`

  * Type: UUID, nullable
  * Cardinality: `0..1`
  * References: `location(id)`
  * Description: Optional self-reference indicating that this location is part of another location.

---

# Repeating coded concepts

## Table: `location_accessibility_extension`

This table stores the array of accessibility extensions associated with a location.

### Purpose

Represents zero or more accessibility characteristics for a location using coded concepts, such as:

* handicapped accessible
* ADA compliant
* public transport options
* entry service
* cognitive mobility
* cultural competence

### Fields

* `id`

  * Type: UUID
  * Cardinality: `1..1`
  * Description: Primary key.

* `location_id`

  * Type: UUID
  * Cardinality: `1..1`
  * References: `location(id)`
  * Description: Parent location.

* `accessibility_code`

  * Type: coded concept representation
  * Cardinality: `1..1`
  * Description: Accessibility extension value.

### Implementation note

If you are using the same normalized coded concept pattern as elsewhere in the FHIR project, this field should be modeled the same way as your standard **CodeableConcept** representation rather than as a simple string.

---

# Relationship summary

## Outbound relationships from Location

* `location.address_id -> address.id`
* `location.managing_organization_id -> organization.id`
* `location.part_of_location_id -> location.id`
* `location_accessibility_extension.location_id -> location.id`

## Inbound / bidirectional architecture considerations

To support bidirectional traversal under the NDH standard architecture:

* From **Organization**, you can resolve all managed locations by querying:

  * `location.managing_organization_id = organization.id`

* From a **parent Location**, you can resolve all child locations by querying:

  * `location.part_of_location_id = parent_location.id`

* From **Address**, you can resolve its owning location by querying:

  * `location.address_id = address.id`

* From **Location**, you can resolve all accessibility extensions by querying:

  * `location_accessibility_extension.location_id = location.id`

---

# Recommended PostgreSQL shape

## `location`

* `id uuid primary key`
* `active boolean not null`
* `name text not null`
* `description text null`
* `boundary_geojson jsonb not null`
* `latitude numeric null`
* `longitude numeric null`
* `altitude numeric null`
* `availability_exceptions text null`
* `address_id uuid not null`
* `managing_organization_id uuid null`
* `part_of_location_id uuid null`

## `location_accessibility_extension`

* `id uuid primary key`
* `location_id uuid not null`
* `accessibility_code ... not null`

---

# Constraints and business rules

* Each location must have exactly one address.
* Each location must have exactly one boundary GeoJSON object.
* A location may have zero or one managing organization.
* A location may have zero or one parent location.
* A location may have zero or more accessibility extensions.
* Description is optional.
* Latitude, longitude, and altitude are optional.
* Availability exceptions are optional.
* `part_of_location_id` must not equal the location’s own `id`.
* If geographic point data is supplied, latitude and longitude should normally be provided together.

---

# Suggested notes for implementation

* Use `jsonb` for `boundary_geojson` in PostgreSQL.
* Add a check or application validation to ensure the stored JSON is valid GeoJSON.
* Add indexes on:

  * `managing_organization_id`
  * `part_of_location_id`
  * `address_id`
  * optionally a GIN index on `boundary_geojson` if boundary querying is needed


