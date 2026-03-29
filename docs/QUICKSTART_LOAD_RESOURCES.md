# Quick Start: Loading FHIR Resources with SQL-on-FHIR

## Prerequisites

1. **PostgreSQL running** with connection details in `.env`:
   ```bash
   cp env.example .env
   # Edit .env with your database credentials
   ```

2. **Python virtual environment** with dependencies:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   pip install fhir4ds pandas sqlalchemy psycopg2-binary
   ```

## Loading Test Data

### Option 1: Batch Load (Recommended)

Use the `go.py` script to load all resources at once:

```bash
# Activate virtual environment
source .venv/bin/activate

# Load all test files (*.5.ndjson) from a directory
python scripts/go.py /Volumes/eBolt/palantir/ndjson/initial --test

# Load all production files (*.ndjson, excluding *.###.ndjson)
python scripts/go.py /Volumes/eBolt/palantir/ndjson/initial
```

### Option 2: Individual Resources

Load resources one at a time:

```bash
# Activate virtual environment
source .venv/bin/activate

# 1. Load Endpoints
python scripts/load_endpoint_ndjson.py /Volumes/eBolt/palantir/ndjson/initial/Endpoint.5.ndjson

# 2. Load Locations
python scripts/load_location_ndjson.py /Volumes/eBolt/palantir/ndjson/initial/Location.5.ndjson

# 3. Load Organizations (Clinical)
python scripts/load_organization_ndjson.py /Volumes/eBolt/palantir/ndjson/initial/Organization.5.ndjson

# 4. Load OrganizationAffiliations
python scripts/load_organization_affiliation_ndjson.py /Volumes/eBolt/palantir/ndjson/initial/OrganizationAffiliation.5.ndjson

# 5. Load PractitionerRoles
python scripts/load_practitioner_role_ndjson.py /Volumes/eBolt/palantir/ndjson/initial/PractitionerRole.5.ndjson

# 6. Load Practitioners
python scripts/load_practitioner_ndjson.py /Volumes/eBolt/palantir/ndjson/initial/Practitioner.5.ndjson
```

PROCESSING COMPLETE
Status: success
Total resources in file: 5
Matching [ResourceType]s: 5
Resource type: [ResourceType]
Saved to: postgres.fhirtablesaw.[table_name]
Rows in table (verified): 5
Mode: append

✓ Data successfully loaded to PostgreSQL!
✓ Verified: 5 rows in table
```
## Expected Output

### Batch Loading (go.py)
```
FHIR NDJSON Batch Loader
Directory: /Volumes/eBolt/palantir/ndjson/initial
Mode: TEST MODE (*.5.ndjson)
Table handling: append

Found 6 matching file(s):
  - Endpoint.5.ndjson → endpoint (✓ Supported)
  - Location.5.ndjson → location (✓ Supported)
  ...

BATCH PROCESSING COMPLETE
Files processed: 6
Successful: 6
Errors: 0

✓ Successfully loaded resources:
  - Endpoint.5.ndjson: 5 endpoint(s) → postgres.fhirtablesaw.endpoint
  - Location.5.ndjson: 5 location(s) → postgres.fhirtablesaw.location
  ...
```

### Individual Scripts
Each script will show:
```
PROCESSING COMPLETE
Status: success
Total resources in file: 5
Matching [ResourceType]s: 5
Resource type: [ResourceType]
Saved to: postgres.fhirtablesaw.[table_name]
Rows in table (verified): 5
Mode: append

✓ Data successfully loaded to PostgreSQL!
✓ Verified: 5 rows in table
```
============================================================
PROCESSING COMPLETE
============================================================
Status: success
Total resources in file: 5
Matching [ResourceType]s: 5
Resource type: [ResourceType]
Saved to: postgres.fhirtablesaw.[table_name]
Rows in table (verified): 5
Mode: append
============================================================

✓ Data successfully loaded to PostgreSQL!
✓ Verified: 5 rows in table
```

## Upsert Behavior

All scripts support **upsert** (insert or update):
- First run: Inserts new records
- Subsequent runs: Updates existing records by `resource_uuid`
- No duplicates created

To **replace** all data instead:
```python
# Edit the script and change:
if_exists="append"  # to:
if_exists="replace"
```

## Database Schema

All tables are created in the schema specified by `DB_SCHEMA` environment variable (default: `public`).

Database structure: `{database}.{schema}.{table_name}`

Example: `postgres.fhirtablesaw.location`

## Tables Created

| Table Name | Description | Row Count (test data) |
|------------|-------------|----------------------|
| endpoint | Technical endpoints for interoperability | 5 |
| location | Physical locations for service delivery | 5 |
| organization | Clinical organizations (type='prov') | 5 |
| organization_affiliation | Organization-to-organization bridges | 5 |
| practitioner | Individual healthcare providers | 5 |
| practitioner_role | Practitioner-to-organization bridges | 5 |

## Reference Handling

Reference columns store the full FHIR reference string:
- Example: `"Organization/1003000118"` not just `"1003000118"`

To extract UUIDs in SQL:
```sql
SELECT
    resource_uuid,
    organization_reference,
    split_part(organization_reference, '/', 2) AS organization_uuid
FROM practitioner_role;
```

## Troubleshooting

### Missing Dependencies
```bash
pip install fhir4ds pandas sqlalchemy psycopg2-binary
```

### Database Connection Issues
Check your `.env` file has correct values:
```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=postgres
DB_USER=your_username
DB_PASSWORD=your_password
DB_SCHEMA=fhirtablesaw
```

### Test Connection
```bash
python test_db_connection.py
```

## Files Reference

- **ViewDefinitions**: `viewdefs/*.json` - Define how FHIR flattens to tables
- **Loading Scripts**: `scripts/load_*_ndjson.py` - CLI tools for loading data
- **Integration**: `src/fhir_tablesaw_3tier/fhir4ds_integration.py` - Core processing logic

## See Also

- `TEST_RESULTS.md` - Detailed test results and issues resolved
- `IMPLEMENTATION_SUMMARY_RESOURCES.md` - Full implementation documentation
- `README_SQL_ON_FHIR.md` - General SQL-on-FHIR documentation
