# Implementation Summary: SQL-on-FHIR for 5 Additional Resources

## Overview

This document summarizes the implementation of SQL-on-FHIR ViewDefinitions and loading infrastructure for 5 FHIR resource types:

1. **Endpoint**
2. **Location**
3. **Organization** (Clinical - type='prov')
4. **OrganizationAffiliation**
5. **PractitionerRole**

All implementations follow the same pattern established by the Practitioner resource, using the fhir4ds library with ViewDefinitions to flatten FHIR JSON into PostgreSQL tables.

## Files Created

### ViewDefinitions (viewdefs/)

1. `viewdefs/endpoint.json` - Flattens Endpoint resources
2. `viewdefs/location.json` - Flattens Location resources
3. `viewdefs/organization.json` - Flattens Organization resources (filtered for type='prov')
4. `viewdefs/organization_affiliation.json` - Flattens OrganizationAffiliation resources
5. `viewdefs/practitioner_role.json` - Flattens PractitionerRole resources

### Integration Functions

Added to `src/fhir_tablesaw_3tier/fhir4ds_integration.py`:

- `process_endpoint_ndjson()`
- `process_location_ndjson()`
- `process_organization_ndjson()`
- `process_organization_affiliation_ndjson()`
- `process_practitioner_role_ndjson()`

### Loading Scripts (scripts/)

1. `scripts/load_endpoint_ndjson.py`
2. `scripts/load_location_ndjson.py`
3. `scripts/load_organization_ndjson.py`
4. `scripts/load_organization_affiliation_ndjson.py`
5. `scripts/load_practitioner_role_ndjson.py`

All scripts are executable and follow the same CLI pattern as `load_practitioner_ndjson.py`.

## Implementation Details by Resource

### 1. Endpoint

**Table Name:** `endpoint`

**Key Columns:**
- `resource_uuid` - FHIR resource ID
- `status` - Operational state (required)
- `connection_type_system`, `connection_type_code`, `connection_type_display` - Connection type coding
- `name` - Human-readable name (nullable)
- `address` - Technical endpoint URL
- `endpoint_rank` - NDH endpoint rank extension (nullable)

**Test File:** `/Volumes/eBolt/palantir/ndjson/initial/Endpoint.5.ndjson`

**Usage:**
```bash
python scripts/load_endpoint_ndjson.py /Volumes/eBolt/palantir/ndjson/initial/Endpoint.5.ndjson
```

### 2. Location

**Table Name:** `location`

**Key Columns:**
- `resource_uuid` - FHIR resource ID
- `status`, `name` - Required fields
- `description`, `mode`, `availability_exceptions` - Optional fields
- `address_line1`, `address_line2`, `address_city`, `address_state`, `address_postal_code`, `address_country` - Flattened address
- `latitude`, `longitude`, `altitude` - Geographic position
- `managing_organization_resource_uuid` - Reference to managing org
- `part_of_location_resource_uuid` - Self-reference for hierarchies

**Test File:** `/Volumes/eBolt/palantir/ndjson/initial/Location.5.ndjson`

**Notes:**
- The test data does NOT contain `boundary_geojson` extension (specified as required in AI_Instructions/Location.md but missing in actual data)
- Columns will be NULL where data is missing per your specification

**Usage:**
```bash
python scripts/load_location_ndjson.py /Volumes/eBolt/palantir/ndjson/initial/Location.5.ndjson
```

### 3. Organization (Clinical)

**Table Name:** `organization`

**Key Columns:**
- `resource_uuid` - FHIR resource ID
- `npi` - National Provider Identifier (extracted from identifiers)
- `active`, `name` - Organization status and legal name
- `description` - From NDH org-description extension (nullable)
- `part_of_resource_uuid` - Reference to parent organization
- `organization_type_code` - Captured for verification (should be 'prov')

**Filtering:** ViewDefinition includes a WHERE clause to filter only organizations with `type.coding.code = 'prov'`

**Test File:** `/Volumes/eBolt/palantir/ndjson/initial/Organization.5.ndjson`

**Usage:**
```bash
python scripts/load_organization_ndjson.py /Volumes/eBolt/palantir/ndjson/initial/Organization.5.ndjson
```

### 4. OrganizationAffiliation

**Table Name:** `organization_affiliation`

**Key Columns:**
- `resource_uuid` - FHIR resource ID
- `active` - Whether the affiliation is active
- `primary_organization_resource_uuid` - Reference to primary organization (extracted from reference)
- `participating_organization_resource_uuid` - Reference to participating organization
- `code_system`, `code_code`, `code_display` - One affiliation role code (CMS-constrained to 1)

**Test File:** `/Volumes/eBolt/palantir/ndjson/initial/OrganizationAffiliation.5.ndjson`

**Notes:**
- Test data shows minimal OrganizationAffiliation records with only active flag and organization references
- Code fields may be NULL if not present in source data

**Usage:**
```bash
python scripts/load_organization_affiliation_ndjson.py /Volumes/eBolt/palantir/ndjson/initial/OrganizationAffiliation.5.ndjson
```

### 5. PractitionerRole

**Table Name:** `practitioner_role`

**Key Columns:**
- `resource_uuid` - FHIR resource ID
- `active` - Whether the role is active
- `practitioner_resource_uuid` - Reference to Practitioner (extracted from reference)
- `organization_resource_uuid` - Reference to Organization (extracted from reference)
- `code_system`, `code_code`, `code_display` - One NDH practitioner role code
- `location_resource_uuid` - Reference to Location (CMS-constrained to 0..1)
- `healthcare_service_resource_uuid` - Reference to HealthcareService (CMS-constrained to 0..1)
- `accepting_new_patients` - From newpatients extension (nullable)
- `rating` - From rating extension (nullable)

**Test File:** `/Volumes/eBolt/palantir/ndjson/initial/PractitionerRole.5.ndjson`

**Notes:**
- Test data shows the newpatients extension present
- Specialty codes are present but not extracted in initial ViewDefinition (can be added later)

**Usage:**
```bash
python scripts/load_practitioner_role_ndjson.py /Volumes/eBolt/palantir/ndjson/initial/PractitionerRole.5.ndjson
```

## Common Features

All implementations share these characteristics:

1. **Upsert Support:** ON CONFLICT (resource_uuid) DO UPDATE ensures re-running scripts updates existing records
2. **Unique Constraints:** Automatic creation of unique constraint on `resource_uuid`
3. **Reference Extraction:** FHIR references like "Organization/123" are converted to just "123"
4. **NULL Handling:** Missing fields result in NULL columns (not errors)
5. **Snake Case:** All column names use snake_case convention
6. **Error Handling:** Comprehensive error messages for missing files, invalid data, etc.

## Testing the Implementation

### Prerequisites

1. Ensure PostgreSQL is running and connection info is in `.env`:
   ```
   DB_HOST=localhost
   DB_PORT=5432
   DB_NAME=your_database
   DB_USER=your_user
   DB_PASSWORD=your_password
   DB_SCHEMA=public
   ```

2. Ensure fhir4ds is installed:
   ```bash
   pip install fhir4ds pandas sqlalchemy psycopg2-binary
   ```

### Test Each Resource

Run each loading script with its corresponding test file:

```bash
# 1. Endpoint
python scripts/load_endpoint_ndjson.py /Volumes/eBolt/palantir/ndjson/initial/Endpoint.5.ndjson

# 2. Location
python scripts/load_location_ndjson.py /Volumes/eBolt/palantir/ndjson/initial/Location.5.ndjson

# 3. Organization (Clinical)
python scripts/load_organization_ndjson.py /Volumes/eBolt/palantir/ndjson/initial/Organization.5.ndjson

# 4. OrganizationAffiliation
python scripts/load_organization_affiliation_ndjson.py /Volumes/eBolt/palantir/ndjson/initial/OrganizationAffiliation.5.ndjson

# 5. PractitionerRole
python scripts/load_practitioner_role_ndjson.py /Volumes/eBolt/palantir/ndjson/initial/PractitionerRole.5.ndjson
```

### Verification

After running each script, verify:

1. **Script Output:** Should show "✓ Data successfully loaded to PostgreSQL!"
2. **Row Count:** Matches the number of resources in the NDJSON file
3. **Database Tables:** Query PostgreSQL to inspect the data:

```sql
-- Check all tables
SELECT 'endpoint' AS table_name, COUNT(*) FROM endpoint
UNION ALL
SELECT 'location', COUNT(*) FROM location
UNION ALL
SELECT 'organization', COUNT(*) FROM organization
UNION ALL
SELECT 'organization_affiliation', COUNT(*) FROM organization_affiliation
UNION ALL
SELECT 'practitioner_role', COUNT(*) FROM practitioner_role;

-- Sample data from each table
SELECT * FROM endpoint LIMIT 5;
SELECT * FROM location LIMIT 5;
SELECT * FROM organization LIMIT 5;
SELECT * FROM organization_affiliation LIMIT 5;
SELECT * FROM practitioner_role LIMIT 5;
```

### Re-run Test (Upsert Verification)

Re-run any script to verify upsert behavior:

```bash
python scripts/load_endpoint_ndjson.py /Volumes/eBolt/palantir/ndjson/initial/Endpoint.5.ndjson
```

Row count should remain the same (not duplicate).

## Known Limitations and Notes

### 1. Simplified ViewDefinitions

The current ViewDefinitions extract core fields but do not capture:
- **Complex extensions** (stored as JSON in some cases or omitted)
- **Repeating arrays** (specialty, telecom, etc.) - would require separate tables
- **Nested structures** - flattened to single-level columns where possible

This is intentional for the initial implementation following the Practitioner pattern.

### 2. Missing Data in Test Files

Some test files are missing fields specified as required in AI_Instructions:
- **Location:** No `boundary_geojson` extension present
- **OrganizationAffiliation:** No `code` field in test data

These cases result in NULL columns.

### 3. Organization Filtering

The Organization ViewDefinition filters for `type='prov'` only. Other organization types would require:
- Separate ViewDefinitions
- Separate tables (as per ThreeLayerApproach.md guidance)

### 4. Reference Integrity

The ViewDefinitions extract resource UUIDs from references but do not:
- Validate that referenced resources exist
- Create foreign key constraints (per AGENTS.md: "Do not implement Foreign Keys")

## Next Steps

Potential enhancements:

1. **Add Repeating Data Support:** Create separate tables for arrays (specialty, telecom, endpoints, etc.)
2. **Add Extension Handling:** Extract additional NDH extensions
3. **Create Views:** SQL views joining related tables
4. **Add Validation:** Pre-load validation of FHIR resources
5. **Batch Loading:** Scripts to load all resources in order
6. **Data Quality Reports:** Analyze missing required fields, broken references, etc.

## Architecture Alignment

This implementation follows:

- ✅ **AGENTS.md:** SQL-on-FHIR ViewDefinition approach
- ✅ **ThreeLayerApproach.md:** Separate tables for different organization types
- ✅ **CommonRules.md:** Snake_case naming, no CSV reading, uppercase SQL keywords
- ✅ **AI_Instructions/*.md:** Resource-specific requirements and field mappings

All code is in `src/fhir_tablesaw_3tier/` separate from the legacy `src/fhir_tablesaw/` codebase.
