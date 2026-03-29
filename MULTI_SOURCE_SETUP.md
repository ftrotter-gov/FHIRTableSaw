# Multi-Source FHIR Data Processing

This document describes the multi-source configuration system that allows processing FHIR data from three separate sources: Test Server, CMS, and Palantir.

## Overview

FHIRTableSaw now supports processing data from three independent sources, each with:
- **Separate data directories** for NDJSON, DuckDB, and CSV files
- **Separate database schemas** for PostgreSQL uploads
- **Dedicated wrapper scripts** for easy execution
- **Intelligent resume** capabilities to skip completed stages

## Configuration

### Environment Variables

Configure your `.env` file with the following variables:

```bash
# Data directories for each source
TEST_FHIR_DIR=/path/to/test/fhir/data
CMS_FHIR_DIR=/path/to/cms/fhir/data
PALANTIR_FHIR_DIR=/path/to/palantir/fhir/data

# Database schemas for each source
TEST_FHIR_SCHEMA=fhirtablesaw_test
CMS_FHIR_SCHEMA=fhirtablesaw_cms
PALANTIR_FHIR_SCHEMA=fhirtablesaw_palantir

# Common configuration
DATABASE_URL=postgresql+psycopg://user:password@localhost:5432/dbname
FHIR_SERVER_URL=https://dev.cnpd.internal.cms.gov/fhir/
FHIR_API_USERNAME=your_username
FHIR_API_PASSWORD=your_password
```

**Important:** Replace `/path/to/...` with actual directory paths. The scripts will validate that these are set and are not placeholder values like `/REPLACEME/`.

### Legacy Variables

For backward compatibility, the following variables are still supported:
- `FHIR_API_CACHE_FOLDER` - Falls back if using `go.py` directly
- `DB_SCHEMA` - Falls back if using `go.py` directly

## Usage

### Three Dedicated Scripts

Instead of manually configuring each run, use the dedicated wrapper scripts:

#### 1. Test Server Data

```bash
python go_testserver.py [OPTIONS]
```

Automatically uses:
- Data directory: `TEST_FHIR_DIR`
- Database schema: `TEST_FHIR_SCHEMA`

#### 2. CMS Data

```bash
python go_cms.py [OPTIONS]
```

Automatically uses:
- Data directory: `CMS_FHIR_DIR`
- Database schema: `CMS_FHIR_SCHEMA`

#### 3. Palantir Data

```bash
python go_p.py [OPTIONS]
```

Automatically uses:
- Data directory: `PALANTIR_FHIR_DIR`
- Database schema: `PALANTIR_FHIR_SCHEMA`

### Common Options

All wrapper scripts accept the same options as `go.py`:

```bash
# Download options
--count <n>                    # FHIR paging size (default: 1000)
--stop-after-this-many <n>     # Stop after N resources (testing)
--resource-types <types>       # Comma-separated resource types

# Processing options
--batch-size <n>               # Resources per batch (default: 5000)
--limit <n>                    # Max resources to process (testing)
--upload-mode <mode>           # PostgreSQL mode: replace/append/fail
--no-upload                    # Skip PostgreSQL upload

# Advanced options
--temp-dir <path>              # DuckDB temp directory
--log-dir <path>               # Error log directory
```

## Intelligent Resume

The system now intelligently detects completed work and resumes from where it left off:

### Download Stage

- **All NDJSON files exist**: Skips download entirely
- **Some NDJSON files exist**: Downloads only missing ones
- **No NDJSON files exist**: Downloads all requested types

### Processing Stage

For each resource type:

- **CSV exists, no upload needed**: Skips processing entirely
- **CSV exists, upload needed**: Re-runs upload only
- **CSV missing**: Runs full pipeline (DuckDB → CSV → Upload)

### Status Display

The scripts show clear status information:

```
================================================================================
FHIRTableSaw go.py runner (with intelligent resume)
================================================================================
FHIR server URL: https://dev.cnpd.internal.cms.gov/fhir/
Cache folder:   /path/to/palantir/fhir/data
DB Schema:      fhirtablesaw_palantir
Postgres upload: YES
================================================================================

Existing NDJSON files detected:
  ✓ Practitioner: practitioner.ndjson (125.3 MB)
  ✓ Organization: organization.ndjson (89.7 MB)

⚠ All NDJSON files exist. Skipping download stage.
  To force re-download, delete the NDJSON files or use a different directory.
================================================================================
```

### Force Re-processing

To force re-processing from scratch:

```bash
# Delete specific artifacts
rm /path/to/data/practitioner.ndjson      # Re-download
rm /path/to/data/practitioner.duckdb      # Re-process to DuckDB
rm /path/to/data/practitioner_*.csv       # Re-export to CSV

# Or delete entire directory
rm -rf /path/to/data/*
```

## Examples

### Process Test Server Data

```bash
# Full pipeline with defaults
python go_testserver.py

# Test with limited resources
python go_testserver.py --stop-after-this-many 1000 --limit 1000

# Process only specific resource types
python go_testserver.py --resource-types Practitioner,Organization

# Skip PostgreSQL upload (CSV only)
python go_testserver.py --no-upload
```

### Process CMS Data

```bash
# Full pipeline
python go_cms.py

# With custom batch size
python go_cms.py --batch-size 10000

# Append mode for PostgreSQL
python go_cms.py --upload-mode append
```

### Process Palantir Data

```bash
# Full pipeline
python go_p.py

# Custom temp directory (for better performance)
python go_p.py --temp-dir /fast/ssd/temp

# Process specific resources only
python go_p.py --resource-types Practitioner,PractitionerRole,Organization
```

## Workflow

### Typical Multi-Source Workflow

1. **Configure .env** with all three sources:
   ```bash
   cp env.example .env
   # Edit .env with actual paths
   ```

2. **Process each source independently:**
   ```bash
   # Process test data
   python go_testserver.py
   
   # Process CMS data  
   python go_cms.py
   
   # Process Palantir data
   python go_p.py
   ```

3. **Re-run for updates** (intelligent resume will skip completed stages):
   ```bash
   # If NDJSON already exists, only processes to CSV/PostgreSQL
   python go_palantir.py
   ```

### Working with Existing NDJSON Files

If you already have NDJSON files in the directories:

1. **Ensure proper naming**: Files must follow the pattern:
   - `practitioner.ndjson`
   - `organization.ndjson`
   - `endpoint.ndjson`
   - etc.

2. **Run the appropriate wrapper script**:
   ```bash
   python go_p.py
   ```

3. **The script will**:
   - Detect existing NDJSON files
   - Skip download stage
   - Process NDJSON → DuckDB → CSV → PostgreSQL
   - Skip stages already completed

## Data Organization

### Directory Structure

Each source maintains its own directory with all artifacts:

```
/path/to/palantir/fhir/data/
├── practitioner.ndjson              # Downloaded FHIR resources
├── practitioner.duckdb              # DuckDB database (intermediate)
├── practitioner_practitioner.csv    # Exported CSV (final)
├── organization.ndjson
├── organization.duckdb
├── organization_organization.csv
└── ... (other resource types)
```

### Database Organization

Each source uses a separate schema in PostgreSQL:

```
Database: ndh
├── Schema: fhirtablesaw_test
│   ├── practitioner
│   ├── organization
│   └── ...
├── Schema: fhirtablesaw_cms
│   ├── practitioner
│   ├── organization
│   └── ...
└── Schema: fhirtablesaw_palantir
    ├── practitioner
    ├── organization
    └── ...
```

## Troubleshooting

### "Missing or invalid TEST_FHIR_DIR"

Ensure your `.env` file has valid directory paths:
```bash
# Bad
TEST_FHIR_DIR=/REPLACEME/

# Good
TEST_FHIR_DIR=/Volumes/eBolt/palantir/ndjson/test
```

### "Missing FHIR_API_CACHE_FOLDER"

If using `go.py` directly (not recommended with multi-source), you must set `FHIR_API_CACHE_FOLDER`:
```bash
FHIR_API_CACHE_FOLDER=/path/to/data
```

Or use the dedicated wrapper scripts instead.

### Partial Processing

If processing fails partway through:

1. **Check the error message** to identify which resource type failed
2. **Fix the issue** (missing dependency, disk space, etc.)
3. **Re-run the script** - intelligent resume will skip completed stages
4. **Delete specific artifacts** if you need to force re-processing:
   ```bash
   rm /path/to/data/problematic_resource.duckdb
   rm /path/to/data/problematic_resource_*.csv
   ```

### Schema Conflicts

If you get schema-related errors:

1. **Check schema exists** in PostgreSQL:
   ```sql
   SELECT schema_name FROM information_schema.schemata;
   ```

2. **Create schema if needed**:
   ```sql
   CREATE SCHEMA IF NOT EXISTS fhirtablesaw_palantir;
   ```

3. **Or let the code create it** (if using SQLAlchemy with appropriate permissions)

## Performance Tips

### Optimize DuckDB Performance

Use a fast SSD for DuckDB temp files:
```bash
python go_p.py --temp-dir /fast/nvme/temp
```

### Batch Size Tuning

- **More memory available**: Increase batch size
  ```bash
  python go_p.py --batch-size 10000
  ```

- **Limited memory**: Decrease batch size
  ```bash
  python go_p.py --batch-size 2000
  ```

### Parallel Processing

Process different sources simultaneously on different machines or terminals:

```bash
# Terminal 1
python go_testserver.py

# Terminal 2
python go_cms.py

# Terminal 3
python go_p.py
```

## Migration from Old Setup

If you have existing data in `FHIR_API_CACHE_FOLDER`:

### Option 1: Move Data

```bash
# Move to new location
mv $OLD_CACHE_FOLDER /path/to/palantir/fhir/data

# Update .env
PALANTIR_FHIR_DIR=/path/to/palantir/fhir/data
```

### Option 2: Symlinks

```bash
# Create symlink
ln -s $OLD_CACHE_FOLDER /path/to/palantir/fhir/data

# Update .env
PALANTIR_FHIR_DIR=/path/to/palantir/fhir/data
```

### Option 3: Continue Using Old Setup

Keep using `go.py` directly with `FHIR_API_CACHE_FOLDER` - it still works for single-source workflows.

## Architecture

### Script Hierarchy

```
go_testserver.py  ──┐
go_cms.py        ──┼──> go.py (main logic)
go_p.py          ──┘

Each wrapper:
1. Loads .env
2. Gets source-specific config
3. Sets FHIR_API_CACHE_FOLDER and DB_SCHEMA
4. Calls go.main() with CLI args
```

### Processing Pipeline

```
NDJSON (exists?) ──> DuckDB ──> CSV (exists?) ──> PostgreSQL
     │                              │
     └─ Skip download if exists     └─ Skip processing if exists
                                       (unless upload needed)
```

## See Also

- [DEPENDENCY_MANAGEMENT.md](DEPENDENCY_MANAGEMENT.md) - Dependency checking system
- [GO_FAST_ENV_CONFIG.md](GO_FAST_ENV_CONFIG.md) - Fast pipeline configuration
- [README.md](README.md) - General project documentation
