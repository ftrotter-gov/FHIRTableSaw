# Development Plan for analysis_neo4j

## Purpose

Create a self-contained Neo4j analysis subproject under analysis_neo4j inside the larger repository.

The goal is to keep all Neo4j-specific Docker, schema, and import logic isolated in one subdirectory so the rest of the project can continue to operate independently.

---

## Basic Structure

The work should live under:

- analysis_neo4j/

That subdirectory should contain:

- standard Neo4j Docker configuration
- import scripts
- Cypher schema and index definitions
- a README explaining startup, import, and reset steps

This should be treated as a subproject, not a rewrite of the parent repository.

---

## Docker Approach

Reuse the standard Neo4j Docker setup as much as possible.

That means:

- use the official Neo4j Docker image
- use Docker Compose rather than a heavily customized container
- mount the normal Neo4j data, logs, and import directories
- avoid custom Docker builds unless they become strictly necessary

The intent is to keep the deployment close to vanilla Neo4j so it is easy to understand, reproduce, and maintain.

---

## Import Scope

The project should include import scripts for NDH-compatible FHIR NDJSON resources for:

- Practitioner
- PractitionerRole
- Organization
- OrganizationAffiliation
- Endpoint
- Location

The scripts should scan a given directory and detect files based on exact resource-type filename prefixes.

Important matching rules:

- OrganizationAffiliation.ndjson must not be treated as Organization
- PractitionerRole.ndjson must not be treated as Practitioner

So matching should be exact by full resource type name, not loose prefix matching.

---

## LLM-Guided Script Generation

Before building the import scripts, the LLM should inspect the source data.

Instruction:

- for each supported resource type, read the first 10 lines of the corresponding NDJSON file in the given directory
- use those sample lines to understand the actual field structure
- then generate or refine the import logic based on what is actually present in the files

This should be done separately for:

- Practitioner
- PractitionerRole
- Organization
- OrganizationAffiliation
- Endpoint
- Location

The purpose is to ensure the import logic reflects the real NDJSON structure instead of relying only on assumptions about generic FHIR.

---

## Import Behavior

The import scripts should:

- locate the supported NDJSON files in a given directory
- parse them one JSON object per line
- create or merge nodes for each resource
- preserve core FHIR identity and reference information
- create relationships where FHIR references are available and useful

At minimum, the import should preserve:

- FHIR resource id
- identifier system and value pairs
- NPI values where present
- major cross-resource references

The scripts should be written so they can be rerun safely without creating uncontrolled duplication.

---

## Indexing Requirements

There should be indexes on:

- all FHIR identifiers
- all NPIs

That includes:

- resource-level FHIR ids
- identifier values and systems
- NPI values wherever they appear in the imported resources

The schema should make identifier lookup efficient both for general FHIR identifier searches and for NPI-specific searches.

---

## Development Phases

### Phase 1: Subproject setup

- create analysis_neo4j
- add standard Neo4j Docker Compose files
- confirm local startup and persistence

### Phase 2: Source inspection

- identify the NDJSON files by exact resource type
- read the first 10 lines for each resource file
- use that inspection to design the import mappings

### Phase 3: Import implementation

- build import scripts for the six supported resource types
- ensure exact filename matching
- load nodes and core relationships

### Phase 4: Schema and indexing

- add constraints and indexes
- index all FHIR identifiers
- index all NPIs

### Phase 5: Validation

- confirm that OrganizationAffiliation is not misclassified as Organization
- confirm that PractitionerRole is not misclassified as Practitioner
- confirm identifiers and NPIs are queryable and indexed
- verify imports can be rerun safely

---

## Summary

This plan creates a Neo4j subproject under analysis_neo4j that:

- stays isolated from the main project
- reuses standard Neo4j Docker as much as possible
- imports NDH-compatible FHIR NDJSON resources
- uses exact resource-type matching to avoid false imports
- instructs the LLM to read the first 10 lines of each NDJSON resource file before generating import logic
- adds indexes for all FHIR identifiers and all NPIs

If you want, I can turn this into a slightly more formal README-style project brief next.
