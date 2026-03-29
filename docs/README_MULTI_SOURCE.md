# Quick Start: Multi-Source Processing

This guide shows how to quickly get started with processing FHIR data from three separate sources.

## 1. Configure Environment

```bash
# Copy example and edit
cp env.example .env
```

Edit `.env` and set your paths:

```bash
# REQUIRED: Set actual directory paths (not /REPLACEME/)
TEST_FHIR_DIR=/Volumes/eBolt/test/fhir
CMS_FHIR_DIR=/Volumes/eBolt/cms/fhir
PALANTIR_FHIR_DIR=/Volumes/eBolt/palantir/ndjson/initial

# REQUIRED: Database schemas
TEST_FHIR_SCHEMA=fhirtablesaw_test
CMS_FHIR_SCHEMA=fhirtablesaw_cms
PALANTIR_FHIR_SCHEMA=fhirtablesaw_palantir

# REQUIRED: Database connection
DATABASE_URL=postgresql+psycopg://user:password@localhost:5432/dbname

# REQUIRED: FHIR API credentials (if downloading)
FHIR_API_USERNAME=your_username
FHIR_API_PASSWORD=your_password
```

## 2. Install Dependencies

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install all dependencies
pip install -r requirements.txt
```

## 3. Process Data

### If NDJSON Files Already Exist

If you already have NDJSON files in the directories:

```bash
# Process Palantir data (most common scenario)
python go_p.py

# Process Test server data
python go_testserver.py

# Process CMS data
python go_cms.py
```

The scripts will:
- ✓ Detect existing NDJSON files
- ✓ Skip download stage
- ✓ Process NDJSON → DuckDB → CSV → PostgreSQL
- ✓ Skip stages already completed (intelligent resume)

### If Starting from Scratch

If you need to download data from FHIR API:

```bash
# Download and process in one command
python go_testserver.py

# Or download specific resource types only
python go_cms.py --resource-types Practitioner,Organization
```

## 4. Verify Results

### Check CSV Files

```bash
# List CSV files created
ls -lh $PALANTIR_FHIR_DIR/*.csv

# Preview a CSV
head -1000 $PALANTIR_FHIR_DIR/practitioner_practitioner.csv
```

### Check PostgreSQL

```sql
-- Connect to database
psql -h localhost -U user -d dbname

-- List schemas
\dn

-- Check tables in Palantir schema
SET search_path TO fhirtablesaw_palantir;
\dt

-- Count records
SELECT COUNT(*) FROM practitioner;
```

## Common Scenarios

### Scenario 1: You Have NDJSON, Need CSV

```bash
# Process existing NDJSON to CSV (no upload)
python go_p.py --no-upload
```

Result:
- Skips download (NDJSON exists)
- Creates DuckDB databases
- Exports CSVs
- Skips PostgreSQL upload

### Scenario 2: You Have NDJSON and CSV, Need PostgreSQL

```bash
# Upload existing CSVs to PostgreSQL
python go_p.py
```

Result:
- Skips download (NDJSON exists)
- Skips DuckDB/CSV (CSV exists)
- Uploads to PostgreSQL

### Scenario 3: Resume After Failure

If processing failed halfway through:

```bash
# Just re-run the same command
python go_p.py
```

Result:
- Skips completed resource types
- Processes only failed/incomplete ones
- Shows summary of skipped vs processed

### Scenario 4: Update One Resource Type

```bash
# Re-download and process just Practitioners
rm $PALANTIR_FHIR_DIR/practitioner.*
python go_p.py --resource-types Practitioner
```

## File Naming Conventions

The system expects and creates files with these patterns:

```
<resource_type_snake_case>.ndjson        # Source data
<resource_type_snake_case>.duckdb        # DuckDB database  
<resource_type_snake_case>_<view_name>.csv  # Exported CSV
```

Examples:
- `practitioner.ndjson` → `practitioner.duckdb` → `practitioner_practitioner.csv`
- `organization.ndjson` → `organization.duckdb` → `organization_organization.csv`
- `practitioner_role.ndjson` → `practitioner_role.duckdb` → `practitioner_role_practitionerrole.csv`

## Troubleshooting Quick Reference

| Error | Solution |
|-------|----------|
| Missing dependencies | Run `pip install -r requirements.txt` |
| Invalid TEST_FHIR_DIR | Edit `.env` with real path (not `/REPLACEME/`) |
| NDJSON not found | Check file naming or run with download |
| PostgreSQL connection failed | Check `DATABASE_URL` in `.env` |
| Schema doesn't exist | Create schema in PostgreSQL or check permissions |
| Out of disk space | Use `--temp-dir` on larger volume |
| Process crashed halfway | Just re-run - intelligent resume handles it |

## Performance Expectations

With the FAST pipeline (DuckDB method):

| Resource Type | Typical Time | Throughput |
|---------------|--------------|------------|
| Practitioner (100K) | ~30 seconds | ~3,300/sec |
| Organization (50K) | ~15 seconds | ~3,300/sec |
| Full pipeline (6 types) | ~2-5 minutes | Varies |

*Times are approximate and depend on hardware, batch size, and data complexity.*

## Next Steps

- See [MULTI_SOURCE_SETUP.md](MULTI_SOURCE_SETUP.md) for detailed documentation
- See [DEPENDENCY_MANAGEMENT.md](DEPENDENCY_MANAGEMENT.md) for dependency details
- See [GO_FAST_ENV_CONFIG.md](GO_FAST_ENV_CONFIG.md) for performance tuning
