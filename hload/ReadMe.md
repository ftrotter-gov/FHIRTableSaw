
# HAPI Loader Study

This directory contains tools for loading FHIR resources into a HAPI FHIR server.

## Tools

### bulk_import_loader.py

A Python script that uses the `hapi-fhir-cli bulk-import` command to load NDJSON files into a HAPI FHIR server in the correct order to maintain referential integrity.

**Features:**

- Uses `util/ndjson_discovery.py` to automatically discover NDJSON files
- Loads resources in the correct order: Organization → Location → Endpoint → Practitioner → OrganizationAffiliation → PractitionerRole
- Creates temporary subdirectories with symlinks (no file copying)
- Configurable with sensible defaults
- Auto-cleanup of temporary directories
- Detailed progress reporting

**Requirements:**

- `hapi-fhir-cli` installed and available in PATH (or specify path with `--cli-path`)
- Python 3.6+
- HAPI FHIR server running (default: http://localhost:8080/fhir)

**Usage:**

```bash
# Basic usage
python hload/bulk_import_loader.py /path/to/ndjson/files

# With custom HAPI server URL
python hload/bulk_import_loader.py /path/to/ndjson --target-url http://localhost:8080/fhir

# With verbose output
python hload/bulk_import_loader.py /path/to/ndjson --verbose

# Keep temporary directories for debugging
python hload/bulk_import_loader.py /path/to/ndjson --no-cleanup

# Continue on error instead of stopping
python hload/bulk_import_loader.py /path/to/ndjson --continue-on-error

# Show all options
python hload/bulk_import_loader.py --help
```

**How it works:**

1. Discovers NDJSON files using naming conventions (ResourceType.ndjson, ResourceType.descriptor.ndjson)
2. For each resource type in order:
   - Creates temporary subdirectory: `.bulk_import_tmp_{ResourceType}/`
   - Creates symlink to the NDJSON file in the temp directory
   - Runs `hapi-fhir-cli bulk-import` on that directory
   - Waits for completion
   - Cleans up temporary directory (unless `--no-cleanup` is specified)

### load_from_dir.sh

A bash script that loads FHIR resources using direct HTTP API calls (curl).

**Usage:**

```bash
./hload/load_from_dir.sh /path/to/ndjson/files
```

### run_hapi_docker.sh

Starts or creates a HAPI FHIR server Docker container.

### hard_delete_hapi_docker.sh

Completely removes the HAPI FHIR Docker container and data.

### load_ig.sh

Uploads an implementation guide package to the HAPI server.

## File Naming Convention

All tools follow the naming conventions from `NamingConventions.md`:

- `ResourceType.ndjson` (exact match, preferred)
- `ResourceType.descriptor.ndjson` (with descriptor)
- `ResourceType.descriptor1.descriptor2.ndjson` (multiple descriptors)

Only `.ndjson` files are processed (NOT `.ndjson.gz`)

=====================

