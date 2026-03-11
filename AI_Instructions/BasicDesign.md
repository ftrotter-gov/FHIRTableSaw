## Project: FHIR-to-Postgres “Aggressive Flattener” for R4 HAPI (No-Foreign-Keys Schema)

### Goal

Create a toolchain that:

1. **Profiles** an existing **FHIR R4 HAPI** server by sampling real data and inferring practical cardinalities and subtype usage (what *actually appears*).
2. Produces an **editable configuration file** (JSON or YAML) that captures:

   * Observed statistics (presence rates, array length distribution, reference usage)
   * Proposed modeling decisions (flatten vs relationalize, join strategy, naming)
3. Uses that edited config to **generate**:

   * Postgres **DDL** (tables + indexes only; **no foreign keys**)
   * A data ingestion “scraper” that pulls from the FHIR REST API and **populates** those tables
     (MVP can generate a Python loader; production loader will be generated in **Go** for millions of records).

### Non-goals (MVP)

* No preservation of FHIR `meta.*`.
* No attempt to cover all of FHIR; only model what appears in sampled data.
* No extensions (or any element) unless they appear in sampled data.
  * **Note:** Extensions are modeled when present, but can be excluded via `ignore_extensions.yaml`.
* No referential integrity enforcement by the database (no FK constraints). Integrity, if needed, is handled by loader logic and/or downstream analytics.

---

## Architecture Overview

### Stage A — Python Profiler (“Compiler front-end”)

**Input:** FHIR base URL; optional auth; sampling configuration.

**Outputs (Stage A must emit all of these):**

1) `model-config.yaml`
   * Project-wide configuration, discovery snapshot, sampling knobs, heuristic settings.
   * Observed stats and proposed modeling decisions/overrides.
2) `table-schema.yaml`
   * **Standalone** relational schema metadata sufficient to generate Postgres DDL.
   * Contains tables, columns, primary keys, indexes, and FHIR source mappings.
3) `ignore_extensions.yaml`
   * List of extension URL patterns (supports wildcards) that are ignored everywhere they appear.
   * Seeded with NDH: `http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-contactpoint-availabletime`

Responsibilities:

* Discover resource types via **CapabilityStatement** (`GET /metadata`).
* For each resource type, retrieve up to **N=1000** resources (configurable) using paging.
* Traverse each resource JSON and:

  * Track **array cardinality distributions** for every array path.
  * Track **presence rates** for encountered paths.
  * Track **reference usage patterns** (target types and frequency) based on `Reference.reference`.
* Infer **Postgres column types** during profiling (Stage A).
* Emit a proposed model using aggressive flattening heuristics, plus **relationship tables** that store IDs but do not declare FKs.

Key relational conventions required by this project:

* **Surrogate primary key everywhere:** every table has `id bigserial primary key`.
* For FHIR resources, store the original FHIR `Resource.id` separately (e.g., `fhir_id text`).
* Join tables store **surrogate ids** (bigint) of the related entities (no FKs declared).
* Dedupe/upsert is accomplished by generating **unique constraints/indexes** (Postgres 15+), including null semantics:
  * Use `UNIQUE NULLS NOT DISTINCT` to treat NULLs as equal for deduplication.

### Stage B — Code Generator (“Compiler back-end”)

**Input:** Edited `model-config.yaml/json`.
**Outputs:**

1. `schema.sql` (Postgres DDL **without** foreign keys)
2. `ingest.go` (production loader; MVP may also emit `ingest.py`)

Responsibilities:

* Generate tables (resources, child tables, join tables) using **ID columns** to represent relationships.
* Generate indexes to support joins, but **never** generate `FOREIGN KEY` constraints.
* Generate loader code that populates tables and join tables consistently.

---

## Stage A: FHIR Discovery & Sampling

### A1. Capability Discovery

* Call `GET {base}/metadata`.
* Parse `CapabilityStatement.rest[].resource[]` to obtain supported resource types.
* Default: include resource types supporting `search-type` (configurable).

### A2. Sampling Strategy

Default sampling per type:

* Use `GET {base}/{ResourceType}?_count={pageSize}` with paging via `Bundle.link[relation="next"]`.
* Pull up to:

  * `maxResourcesPerType = 1000` (default; configurable)
  * `pageSize = 200` (configurable)

Configurable knobs:

* `resourceAllowList` / `resourceDenyList`
* `maxResourcesPerType`
* `pageSize`
* `timeout`, `retry`, `rateLimitQps`

### A3. Ignore-unless-present Rule

* The profiler only models paths that appear in sampled resources.
* No schema inference from IG/StructureDefinition for absent paths.

---

## Stage A: Statistics Collected

### A4. Path Model

Use normalized paths:

* Object: `Practitioner.name`
* Nested: `Practitioner.name.family`
* Array: `Practitioner.telecom[]`
* Array element: `Practitioner.telecom[].value`

### A5. Cardinality Metrics (primary)

For every array path:

* `n_records_seen`
* `count_0`, `count_1`, `count_many`
* Derived `pct_0`, `pct_1`, `pct_many`

For non-array paths:

* `present_count`, `absent_count`
* Derived presence percentage

### A6. Reference Usage Metrics

For any detected `Reference`:

* Count occurrences by target resource type (parsed from `"reference": "Type/id"`).
* Track whether it appears as scalar vs inside arrays.

Purpose: infer shared-entity relationships (e.g., Endpoint) and join table patterns—again, **without foreign keys**.

---

## Stage A: Model Inference Heuristics

### A7. Model Outputs

The profiler proposes:

* Resource tables (one per FHIR resource type used)
* Flattened columns for 1:1-ish nested structures
* Child tables for parent-owned repeating components
* Join tables for many-to-many relationships (especially references to shared resources)
* Singular nouns for table names

### A8. Aggressive Flattening Rule

For any array path, decide flatten vs relationalize:

Default thresholds (configurable):

* Flatten if `pct_many < 10%` **AND** `count_many < minManyCount`

  * `manyPctThreshold = 0.10`
  * `minManyCount = 25` (example default; configurable)
* Otherwise relationalize.

### A9. Relationship Representation (No FK Constraints)

All relationships are represented by plain columns storing IDs, plus join tables as needed. The schema **must not** include `FOREIGN KEY (...) REFERENCES ...`.

Two relational patterns:

#### Pattern 1 — Child table (parent-owned repeats; preferred when “married to parent”)

Use when repeated elements are inline components and appear owned by the parent.

Child table structure (surrogate-id style):

* `id bigserial primary key`
* `{parent}_parent_id bigint` — the parent table surrogate id (e.g., `organization_parent_id`)
* Optional `idx` (integer) — ordinal within the parent array (optional; not always required when deduping)
* Flattened component fields as columns
* Dedup/upsert:
  * Add a unique constraint/index over the component columns (and include `{parent}_parent_id` if the row is truly parent-owned)
  * Use Postgres 15+ `UNIQUE NULLS NOT DISTINCT`

**No FK constraints** are declared. Instead:

* Add indexes on `parent_id` for join performance.

#### Pattern 2 — Many-to-many join tables (shared entities)

Use when repeated items are references to shared resources (notably Endpoint) or otherwise shared.

Join table structure (surrogate-id style):

* `id bigserial primary key`
* `{left}_id bigint`
* `{right}_id bigint`
* Optional relationship attributes (if present)
* Dedup/upsert:
  * Unique constraint on `({left}_id, {right}_id, ...)` using `UNIQUE NULLS NOT DISTINCT`

**No FK constraints** are declared. Instead:

* Add indexes on `left_id` and `right_id`.

### A10. “One endpoint table” rule

* Always unify Endpoint into a single table named `endpoint`.
* Any resource linking to Endpoint results in an appropriate join table (e.g., `organization_endpoint`, `practitionerrole_endpoint`) storing IDs only.

### A10b. NDH carve-out: Organization subtyping

The NDH specification over-uses the `Organization` resource. Stage A must split Organization into sub-entities based on:

* `Organization.type[].coding[].code`

Rules:

* For each observed type code `X`, create an entity/table family `organization_{X}` (e.g., `organization_pay`).
* If an Organization has multiple type codes, it belongs to **all** corresponding subtype entities (it is profiled into each `organization_*`).
* If an Organization has no type, it stays in the base `organization` entity/table.
* Treat each `organization_{X}` as **independent for profiling** (paths/stats/decisions can diverge).

### A11. Flattening nested objects into columns

* Use snake_case columns with prefixes derived from the path.
* Flatten only keys observed in sampled data.

---

## Configuration Files: `model-config.yaml` and `table-schema.yaml` both editable

### A12. File Structure

Sections:

1. `server` (base URL, auth hints, sampling params)
2. `discovery` (resource types included/excluded; capability snapshot)
3. `stats` (observations per resource/path)
4. `model` (decisions + overrides)
5. `naming` (table/column rename rules; singularization)
6. `generation` (DDL + loader options; batching; upsert strategy; **no-fk enforcement**)

Additional file:

* `ignore_extensions.yaml` (list of extension URL patterns to skip everywhere)

### A13. Required override capabilities

User must be able to:

* Force flatten / child-table / join-table per path
* Rename tables/columns
* Drop paths
* Adjust thresholds globally or per resource/path
* Select primary key strategy for child/join tables (composite vs surrogate)
* Specify indexing strategy (since joins rely on indexes, not FKs)

---

## Stage B: Postgres DDL Generation (No Foreign Keys)

### B1. Core requirements

* Generate `CREATE TABLE` statements with:

  * Primary keys
  * Not-null constraints where appropriate (optional in MVP)
  * **Indexes**
* Explicit prohibition:

  * **Do not generate any `FOREIGN KEY` clauses.**
  * **Do not generate `REFERENCES` constraints** of any kind.

### B2. Table conventions

* Resource table:

  * Name: singular (e.g., `organization`, `endpoint`)
  * `id bigserial primary key`
  * `fhir_id text` (unique; used for resolving FHIR references)
* Child table:

  * `{parent}_{component}` (singular)
  * `id bigserial primary key`
  * `{parent}_parent_id bigint`
  * Index on `{parent}_parent_id`
* Join table:

  * `{left}_{right}` (singular)
  * `id bigserial primary key`
  * `{left}_id bigint`, `{right}_id bigint`
  * Indexes on both columns

### B3. Indexes (important since no FKs)

Minimum indexes:

* PK indexes
* For child tables: `index(parent_id)`
* For join tables: `index(left_id)`, `index(right_id)`
  Optional:
* Compound indexes for common query patterns (config-driven)

---

## Stage B: Loader Generation

### B4. Loader responsibilities (FK-free)

The loader must:

* Insert/upsert resources into base tables keyed by `id`
* Insert rows into child/join tables using stored IDs
* Avoid reliance on DB constraints for integrity:

  * If a referenced resource isn’t loaded yet, join rows may still be inserted.
  * Optionally support “deferred validation” mode that reports missing referenced IDs after load.

### B5. MVP loader

* Python loader is acceptable for validation and smaller datasets.
* Uses batching + upserts.

### B6. Production loader (Go)

* Generated Go code for scale:

  * Concurrency, batching, retry/backoff, rate limiting
  * Bulk insert strategies
* Must not depend on foreign keys (since none exist).

---

## Integrity & Validation (Optional, config-driven)

Because the DB won’t enforce referential integrity:

* Provide optional post-load validation reports:

  * Missing target IDs for join rows
  * Orphan child rows (parent_id not present)
* These are **reports**, not constraints.

---

## Acceptance Criteria

* Tool discovers resources from `/metadata`.
* Samples up to N resources per type.
* Emits two config files with array cardinality stats.
* Proposes flatten/relational decisions using the 10% + count thresholds.
* Generates Postgres schema with:

  * One `endpoint` table
  * Join tables for endpoint relationships
  * Child tables for parent-owned repeats
  * **No foreign keys anywhere**
  * Indexes sufficient for join performance
* Loader populates tables from the FHIR API and supports large-scale ingestion via Go.

## Sample yaml

### table-schema.yaml

```yaml
version: 0.1
database: postgres
schema_name: public
no_foreign_keys: true
postgres_version_min: 15

dedupe:
  unique_nulls_not_distinct: true

tables:

  endpoint:
    description: "FHIR Endpoint resources (shared dimension table)."
    primary_key:
      strategy: "surrogate"
      columns: ["id"]

    columns:
      - name: id
        type: bigserial
        nullable: false

      - name: fhir_id
        type: text
        nullable: false
        source:
          resource: "Endpoint"
          path: "Endpoint.id"

      - name: address
        type: text
        nullable: true
        source:
          resource: "Endpoint"
          path: "Endpoint.address"

      - name: connection_type_code
        type: text
        nullable: true
        source:
          resource: "Endpoint"
          path: "Endpoint.connectionType.code"

    indexes:
      - name: endpoint_address_idx
        columns: ["address"]

  organization:
    description: "FHIR Organization resources."
    primary_key:
      strategy: "fhir_id"
      columns: ["id"]

    columns:
      - name: id
        type: text
        nullable: false
        source:
          resource: "Organization"
          path: "Organization.id"

      - name: name
        type: text
        nullable: true
        source:
          resource: "Organization"
          path: "Organization.name"

      # Example of an aggressively-flattened singleton array (inline first-only)
      - name: telecom_system
        type: text
        nullable: true
        source:
          resource: "Organization"
          path: "Organization.telecom[0].system"
        notes: "Flattened from telecom[] using inline_first_only."

      - name: telecom_value
        type: text
        nullable: true
        source:
          resource: "Organization"
          path: "Organization.telecom[0].value"
        notes: "Flattened from telecom[] using inline_first_only."

    indexes:
      - name: organization_name_idx
        columns: ["name"]

  organization_endpoint:
    description: "Join table: Organization <-> Endpoint (no FK constraints)."
    primary_key:
      strategy: "surrogate"
      columns: ["id"]

    columns:
      - name: id
        type: bigserial
        nullable: false

      - name: organization_id
        type: bigint
        nullable: false
        source:
          relationship:
            left_table: "organization"
            left_id_column: "id"

      - name: endpoint_id
        type: bigint
        nullable: false
        source:
          relationship:
            right_table: "endpoint"
            right_id_column: "id"

    indexes:
    unique_constraints:
      - name: organization_endpoint_uniq
        columns: ["organization_id", "endpoint_id"]
        nulls_not_distinct: true
      - name: organization_endpoint_org_idx
        columns: ["organization_id"]
      - name: organization_endpoint_ep_idx
        columns: ["endpoint_id"]

  practitionerrole:
    description: "FHIR PractitionerRole resources."
    primary_key:
      strategy: "fhir_id"
      columns: ["id"]

    columns:
      - name: id
        type: text
        nullable: false
        source:
          resource: "PractitionerRole"
          path: "PractitionerRole.id"

      - name: practitioner_id
        type: text
        nullable: true
        source:
          resource: "PractitionerRole"
          path: "PractitionerRole.practitioner.reference"
          transform: "extract_id"

      - name: organization_id
        type: text
        nullable: true
        source:
          resource: "PractitionerRole"
          path: "PractitionerRole.organization.reference"
          transform: "extract_id"

    indexes:
      - name: practitionerrole_practitioner_id_idx
        columns: ["practitioner_id"]
      - name: practitionerrole_organization_id_idx
        columns: ["organization_id"]

  practitionerrole_endpoint:
    description: "Join table: PractitionerRole <-> Endpoint (no FK constraints)."
    primary_key:
      strategy: "composite"
      columns: ["practitionerrole_id", "endpoint_id"]

    columns:
      - name: practitionerrole_id
        type: text
        nullable: false
      - name: endpoint_id
        type: text
        nullable: false

    indexes:
      - name: practitionerrole_endpoint_pr_idx
        columns: ["practitionerrole_id"]
      - name: practitionerrole_endpoint_ep_idx
        columns: ["endpoint_id"]
```

### table-schema.yaml

```yaml
version: 0.1

server:
  base_url: "https://example-hapi.fhir.org/fhir"
  fhir_version: "R4"
  auth:
    type: "none"  # none | bearer
    token: null

discovery:
  method: "CapabilityStatement"
  require_search_type: true
  include_resource_types: []
  exclude_resource_types: []

sampling:
  max_resources_per_type: 1000
  page_size: 200
  rate_limit_qps: 5
  timeout_seconds: 30

heuristics:
  many_pct_threshold: 0.10
  min_many_count: 25
  aggressive_flattening: true
  singleton_array_flatten_strategy: "inline_first_only"  # inline_first_only | inline_jsonb | child_table
  endpoint_unification: true

generation:
  emit_schema_metadata: true
  schema_metadata_path: "schema-metadata.yaml"
  emit_sql_ddl: true
  ddl_output_path: "schema.sql"
  emit_loader: true
  loader_language: "go"     # python | go
  loader_output_path: "ingest.go"
  no_foreign_keys: true

validation:
  create_reports: true
  report_orphan_child_rows: true
  report_missing_join_targets: true
  fail_on_missing_reference: false

# -------------------------
# Observations (auto-gen)
# -------------------------
stats:

  Organization:
    record_count_sampled: 842
    arrays:
      Organization.telecom[]:
        count_0: 120
        count_1: 650
        count_many: 72
        pct_many: 0.085
      Organization.endpoint[]:
        count_0: 300
        count_1: 400
        count_many: 142
        pct_many: 0.168
        reference_targets:
          Endpoint: 542

  PractitionerRole:
    record_count_sampled: 1000
    arrays:
      PractitionerRole.endpoint[]:
        count_0: 600
        count_1: 300
        count_many: 100
        pct_many: 0.10
        reference_targets:
          Endpoint: 500

# -------------------------
# Decisions (auto-gen, user-editable)
# These are used to synthesize schema-metadata.yaml
# -------------------------
decisions:

  entities:
    endpoint:
      source_resource: "Endpoint"
      table_name: "endpoint"

    organization:
      source_resource: "Organization"
      table_name: "organization"

    practitionerrole:
      source_resource: "PractitionerRole"
      table_name: "practitionerrole"

  relationships:

    - from_entity: "organization"
      source_path: "Organization.endpoint[]"
      type: "join"
      to_entity: "endpoint"
      join_table: "organization_endpoint"
      left_id_column: "organization_id"
      right_id_column: "endpoint_id"

    - from_entity: "practitionerrole"
      source_path: "PractitionerRole.endpoint[]"
      type: "join"
      to_entity: "endpoint"
      join_table: "practitionerrole_endpoint"
      left_id_column: "practitionerrole_id"
      right_id_column: "endpoint_id"

  flattening:

    - entity: "organization"
      source_path: "Organization.telecom[]"
      decision: "flatten"
      strategy: "inline_first_only"
      output_columns:
        - { name: "telecom_system", source_subpath: "system" }
        - { name: "telecom_value",  source_subpath: "value" }

# -------------------------
# Overrides (user-editable)
# -------------------------
overrides:
  force_child_table:
    - "Organization.telecom[]"
  force_join_table: []
  force_flatten: []
  drop_paths: []
  rename_tables: {}
  rename_columns: {}
  extra_indexes: {}


```
