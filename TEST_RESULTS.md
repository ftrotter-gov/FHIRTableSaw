ge# Test Results: SQL-on-FHIR Loading for 5 Resources

## Test Date
2026-03-22 03:06 AM EST

## Summary
✅ **ALL 5 RESOURCES LOADED SUCCESSFULLY**

Each resource loaded all 5 test records from the corresponding .5.ndjson files into PostgreSQL.

## Test Results by Resource

### 1. Endpoint ✅
- **File**: `/Volumes/eBolt/palantir/ndjson/initial/Endpoint.5.ndjson`
- **Table**: `postgres.fhirtablesaw.endpoint`
- **Status**: SUCCESS
- **Rows Loaded**: 5 / 5
- **Key Columns**: resource_uuid, status, connection_type_*, name, address, endpoint_rank

### 2. Location ✅
- **File**: `/Volumes/eBolt/palantir/ndjson/initial/Location.5.ndjson`
- **Table**: `postgres.fhirtablesaw.location`
- **Status**: SUCCESS
- **Rows Loaded**: 5 / 5
- **Key Columns**: resource_uuid, status, name, address_*, latitude, longitude, managing_organization_reference, part_of_location_reference

### 3. Organization ✅
- **File**: `/Volumes/eBolt/palantir/ndjson/initial/Organization.5.ndjson`
- **Table**: `postgres.fhirtablesaw.organization`
- **Status**: SUCCESS
- **Rows Loaded**: 5 / 5
- **Key Columns**: resource_uuid, npi, active, name, organization_type_code, part_of_reference
- **Note**: All 5 test records had type='prov' (clinical organizations)

### 4. OrganizationAffiliation ✅
- **File**: `/Volumes/eBolt/palantir/ndjson/initial/OrganizationAffiliation.5.ndjson`
- **Table**: `postgres.fhirtablesaw.organization_affiliation`
- **Status**: SUCCESS
- **Rows Loaded**: 5 / 5
- **Key Columns**: resource_uuid, active, primary_organization_reference, participating_organization_reference

### 5. PractitionerRole ✅
- **File**: `/Volumes/eBolt/palantir/ndjson/initial/PractitionerRole.5.ndjson`
- **Table**: `postgres.fhirtablesaw.practitioner_role`
- **Status**: SUCCESS
- **Rows Loaded**: 5 / 5
- **Key Columns**: resource_uuid, active, practitioner_reference, organization_reference, accepting_new_patients

## Issues Encountered and Resolved

### Issue 1: FHIRPath .replace() Not Supported
**Problem**: Initial ViewDefinitions used `.replace('Organization/', '')` to extract UUIDs from references.

**Error**:
```
PostgreSQL execution failed: syntax error at or near "/"
```

**Solution**: Changed all reference columns to store full reference strings (e.g., "Organization/123" instead of "123"). Column names changed from `*_resource_uuid` to `*_reference`.

**Affected ViewDefinitions**:
- location.json (managing_organization_reference, part_of_location_reference)
- organization_affiliation.json (primary/participating_organization_reference)
- practitioner_role.json (practitioner_reference, organization_reference, location_reference, healthcare_service_reference)
- organization.json (part_of_reference)

### Issue 2: WHERE Clause Syntax Not Supported
**Problem**: Organization ViewDefinition included:
```json
"where": [
  {
    "path": "type.coding.where(code='prov').exists()"
  }
]
```

**Error**:
```
syntax error at or near "."
LINE 1: ...AS type.coding_item...
```

**Solution**: Removed WHERE clause. Instead, added `organization_type_code` column to capture the type. Filtering for type='prov' can be done with SQL queries after loading.

## Changes from Original Design

1. **Reference Columns**: Store full FHIR reference strings instead of extracted UUIDs
   - This actually aligns better with FHIR principles
   - UUID extraction can be done in SQL views if needed: `split_part(reference, '/', 2)`

2. **Organization Filtering**: Removed ViewDefinition-level filtering
   - All organizations are loaded
   - `organization_type_code` column allows post-load filtering
   - For clinical-only queries: `SELECT * FROM organization WHERE organization_type_code = 'prov'`

## Verification Queries

To verify the loaded data in PostgreSQL:

```sql
-- Check row counts
SELECT 'endpoint' AS table_name, COUNT(*) FROM endpoint
UNION ALL
SELECT 'location', COUNT(*) FROM location
UNION ALL
SELECT 'organization', COUNT(*) FROM organization
UNION ALL
SELECT 'organization_affiliation', COUNT(*) FROM organization_affiliation
UNION ALL
SELECT 'practitioner_role', COUNT(*) FROM practitioner_role;

-- Sample from each table
SELECT * FROM endpoint LIMIT 2;
SELECT * FROM location LIMIT 2;
SELECT * FROM organization LIMIT 2;
SELECT * FROM organization_affiliation LIMIT 2;
SELECT * FROM practitioner_role LIMIT 2;

-- Check organization types
SELECT organization_type_code, COUNT(*)
FROM organization
GROUP BY organization_type_code;

-- Extract UUIDs from references if needed
SELECT
    resource_uuid,
    practitioner_reference,
    split_part(practitioner_reference, '/', 2) AS practitioner_uuid,
    organization_reference,
    split_part(organization_reference, '/', 2) AS organization_uuid
FROM practitioner_role
LIMIT 5;
```

## Next Steps

1. **Create SQL Views**: Build views that join related tables and extract UUIDs from references
2. **Add Filtering**: Create views for specific organization types (clinical, payer, etc.)
3. **Expand ViewDefinitions**: Add support for repeating fields (telecom, addresses, etc.) in separate tables
4. **Add Extensions**: Extract additional NDH extensions as needed
5. **Batch Loading**: Create scripts to load all resources in dependency order

## Conclusion

All 5 FHIR resource types successfully load into PostgreSQL using SQL-on-FHIR ViewDefinitions. The approach is working correctly with minor adjustments to handle fhir4ds limitations around FHIRPath expression complexity.

**Total Resources Loaded**: 25 records (5 of each type)
**Success Rate**: 100%
**Database**: postgres.fhirtablesaw schema
