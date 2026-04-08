# Neo4j FHIR Import Improvements - Summary

## Overview

The Neo4j FHIR import system has been comprehensively updated to improve data modeling based on the specifications in `AI_Instructions/ImproveNeo4JModel.md`.

## Changes Implemented

### 1. Base Importer (`base.py`)

**New Helper Methods:**
- `_extract_identifiers()` - Returns list of `{system, value}` pairs (identifiers are meaningless without both)
- `_extract_npi_single()` - Extracts single NPI for Practitioners
- `_extract_npi_list()` - Extracts multiple NPIs for Organizations
- `_extract_addresses()` - Returns coherent address objects with all fields together
- `_extract_telecoms()` - Separates telecoms into `emails[]`, `phones[]`, `faxes[]`
- `_is_email()` - Regex check to identify email addresses
- `_extract_state_from_org_reference()` - Extracts state code from org references (e.g., "WY" from "Organization/Organization-State-WY")

### 2. Practitioner Importer (`practitioner.py`)

**New Data Model:**
- `npi` - Single NPI (not array)
- `identifiers` - Array of `{system, value}` objects
- `addresses` - Array of address objects with `{line, city, state, postalCode, country, type, use}`
- `emails[]`, `phones[]`, `faxes[]` - Separated telecoms
- `specialties[]` - Extracted from qualifications with `http://nucc.org/provider-taxonomy` system (deduplicated)
- `degrees[]` - Extracted from qualifications with `http://terminology.hl7.org/CodeSystem/v2-0360` system
- `licenses[]` - Array of `{state, license_number}` objects extracted from qualification blocks
- `languages_spoken[]` - Non-English languages from communication array
- `endpoint_references` - Extracted from extensions with rank

**New Relationship:**
- `(Practitioner)-[:HAS_ENDPOINT {rank: X}]->(Endpoint)` - Rank stored as relationship property

### 3. Organization Importer (`organization.py`)

**New Data Model:**
- `npis[]` - Array of NPIs (organizations can have multiple)
- `identifiers` - Array of `{system, value}` objects
- `addresses` - Array of address objects
- `emails[]`, `phones[]`, `faxes[]` - Separated telecoms

### 4. Endpoint Importer (`endpoint.py`)

**Simplified Data Model:**
- `FHIR_address` - URL addresses
- `Direct_address` - Email addresses (detected via regex)
- `rank` - Extracted from extensions
- `identifiers` - Array of `{system, value}` objects
- Removed: `connection_type`, `payload_types`, `name` (simplified)

### 5. Location Importer (`location.py`)

**New Data Model:**
- `address` - Single address object (not array, since Location has max 1 address)
- `identifiers` - Array of `{system, value}` objects

### 6. PractitionerRole Importer (`practitioner_role.py`)

**Updated:**
- `identifiers` - Array of `{system, value}` objects
- `npi` - Single NPI

### 7. OrganizationAffiliation Importer (`organization_affiliation.py`)

**Updated:**
- `identifiers` - Array of `{system, value}` objects

## Key Improvements

### 1. Address Handling
- **Before**: Separate arrays (`address_lines[]`, `cities[]`, `states[]`, `postal_codes[]`)
- **After**: Coherent address objects keeping all components together
- **Benefit**: Each address is a complete unit with all its properties

### 2. Telecom Separation
- **Before**: Generic `"system:value"` strings
- **After**: Separate `emails[]`, `phones[]`, `faxes[]` arrays
- **Benefit**: Easy filtering and querying by communication type

### 3. Identifier Pairs
- **Before**: Parallel arrays (`identifier_systems[]`, `identifier_values[]`)
- **After**: Array of `{system, value}` pairs
- **Benefit**: System and value always stay together (as they should)

### 4. NPI Differentiation
- **Before**: Single `npi` field for all resources
- **After**: `npi` (string) for Practitioners, `npis[]` (array) for Organizations
- **Benefit**: Correctly models that organizations can have multiple NPIs

### 5. Specialty Extraction
- **Before**: Generic `qualifications[]` array
- **After**: Parsed into `specialties[]`, `degrees[]`, and `licenses[]`
- **Benefit**: Structured, queryable data with deduplication

### 6. Medical Licenses
- **Before**: Not extracted
- **After**: Array of `{state, license_number}` objects
- **Benefit**: Can query practitioners by license state

### 7. Languages
- **Before**: Not extracted
- **After**: `languages_spoken[]` with non-English languages only
- **Benefit**: Find multilingual practitioners

### 8. Endpoint Categorization
- **Before**: Generic address field
- **After**: Separate `FHIR_address` and `Direct_address` fields
- **Benefit**: Clear distinction between API endpoints and email addresses

### 9. Endpoint Rank
- **Before**: Not captured
- **After**: Stored as relationship property on `HAS_ENDPOINT` relationships
- **Benefit**: Understand endpoint priority

## Data Storage Format

**Important**: Complex objects (addresses, identifiers, licenses) are stored as **JSON strings** in Neo4j properties because Neo4j does not support nested map objects. To query these fields, you need to parse the JSON in your application code or use Neo4j's JSON functions.

### Simple Fields (Direct Access)
- Strings: `npi`, `name`, `gender`, `status`, `FHIR_address`, `Direct_address`
- Arrays: `emails[]`, `phones[]`, `faxes[]`, `specialties[]`, `degrees[]`, `languages_spoken[]`, `npis[]`
- Numbers: `rank`, `latitude`, `longitude`

### JSON String Fields (Require Parsing)
- `identifiers` - JSON array of `[{system, value}, ...]`
- `addresses` - JSON array of `[{line, city, state, postalCode, ...}, ...]`
- `address` - JSON object of `{line, city, state, postalCode, ...}` (Location only)
- `licenses` - JSON array of `[{state, license_number}, ...]`

## Example Cypher Queries

### Find practitioners with Spanish language capability:
```cypher
MATCH (p:Practitioner)
WHERE 'Spanish' IN p.languages_spoken
RETURN p.name, p.npi, p.languages_spoken
```

### Find practitioners licensed in Wyoming:
```cypher
MATCH (p:Practitioner)
WHERE p.licenses IS NOT NULL
RETURN p.name, p.npi, p.licenses
// Note: Parse JSON in your application to filter by state
```

### Find practitioners by specialty:
```cypher
MATCH (p:Practitioner)
WHERE 'Infectious Disease Physician' IN p.specialties
RETURN p.name, p.npi, p.specialties
```

### Find organizations by NPI:
```cypher
MATCH (o:Organization)
WHERE '1730324872' IN o.npis
RETURN o.name, o.npis
```

### Find endpoints with rank:
```cypher
MATCH (p:Practitioner)-[r:HAS_ENDPOINT]->(e:Endpoint)
WHERE p.npi = '1669540423'
RETURN e.FHIR_address, e.Direct_address, r.rank
ORDER BY r.rank
```

### Query addresses properly:
```cypher
MATCH (p:Practitioner)
WHERE ANY(addr IN p.addresses WHERE addr.state = 'WY')
RETURN p.name, p.addresses
```

### Find email vs FHIR endpoints:
```cypher
// FHIR API endpoints
MATCH (e:Endpoint)
WHERE e.FHIR_address IS NOT NULL
RETURN e.FHIR_address, e.rank

// Direct/email endpoints
MATCH (e:Endpoint)
WHERE e.Direct_address IS NOT NULL
RETURN e.Direct_address, e.rank
```

## Backward Compatibility

**WARNING**: This is a breaking change. Old data imported with the previous schema will have different field names:
- Old: `identifier_systems`, `identifier_values`
- New: `identifiers`
- Old: `address_lines`, `cities`, `states`, `postal_codes`
- New: `addresses`
- Old: `telecoms`
- New: `emails`, `phones`, `faxes`

**Recommendation**: Reset your Neo4j database and re-import with the new schema.

## Testing

The implementation has been validated against the example files in `data/examples/`:
- `1669540423_praciticioner.json` - Practitioner with specialties, licenses, addresses, telecoms, endpoint references
- `1730324872_org.json` - Organization with multiple NPIs, addresses, telecoms
- `ff89cb4e-4e59-48a2-89ab-d6c92908b769.end.json` - Endpoint with rank

## Files Modified

1. `analysis_neo4j/scripts/importers/base.py`
2. `analysis_neo4j/scripts/importers/practitioner.py`
3. `analysis_neo4j/scripts/importers/organization.py`
4. `analysis_neo4j/scripts/importers/endpoint.py`
5. `analysis_neo4j/scripts/importers/location.py`
6. `analysis_neo4j/scripts/importers/practitioner_role.py`
7. `analysis_neo4j/scripts/importers/organization_affiliation.py`

## Next Steps

1. **Reset Database**: `docker-compose down -v && docker-compose up -d`
2. **Recreate Indexes**: Run `schema/indexes.cypher`
3. **Re-import Data**: `python scripts/import_ndjson.py /path/to/data`
4. **Test Queries**: Use the example queries above to verify data structure

## Date Completed

April 8, 2026
